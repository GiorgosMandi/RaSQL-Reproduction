package org.apache.spark.sql.rasql

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.{Catalog, ComputeCurrentTime, DistinctAggregationRewriter, EliminateSubQueries, FunctionRegistry, HiveTypeCoercion, MultiAlias, NoSuchTableException, ResolveUpCast, UnresolvedAlias, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, CreateStruct, CreateStructUnsafe, Expression, Generator, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.rasql.logical.{MonotonicAggregate, RecursiveRelation}

case class Analyzer(catalog: Catalog,
                    registry: FunctionRegistry,
                    conf: CatalystConf,
                    maxIterations: Int = 100) extends org.apache.spark.sql.catalyst.analysis.Analyzer(catalog, registry, conf, maxIterations) {


    override lazy val batches: Seq[Batch] = Seq(
        Batch("Substitution", fixedPoint,
            CTESubstitution,
            WindowsSubstitution),
        Batch("Resolution", fixedPoint,
            ResolveRelations2 ::
                ResolveReferences ::
                ResolveGroupingAnalytics ::
                ResolvePivot ::
                ResolveUpCast ::
                ResolveSortReferences ::
                ResolveGenerate ::
                ResolveFunctions ::
                ResolveAliases2 ::
                ExtractWindowExpressions ::
                GlobalAggregates ::
                ResolveAggregateFunctions ::
                DistinctAggregationRewriter(conf) ::
                HiveTypeCoercion.typeCoercionRules ++
                    extendedResolutionRules : _*),
        Batch("Nondeterministic", Once, PullOutNondeterministic, ComputeCurrentTime),
        Batch("UDF", Once, HandleNullInputsForUDF),
        Batch("Cleanup", fixedPoint, CleanupAliases2)
    )



    /**
     * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
     */
    object ResolveRelations2 extends Rule[LogicalPlan] {
        def getTable(u: UnresolvedRelation): LogicalPlan = {
            try {
                catalog.lookupRelation(u.tableIdentifier, u.alias)
            } catch {
                case _: NoSuchTableException =>
                    u.failAnalysis(s"Table not found: ${u.tableName}")
            }
        }

        def getTable(rr: RecursiveRelation): LogicalPlan = catalog.lookupRelation(rr.tableIdentifier)

        def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
            case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
                i.copy(table = EliminateSubQueries(getTable(u)))
            case u: UnresolvedRelation  =>
                try {
                    getTable(u)
                } catch {
                    case _: AnalysisException if u.tableIdentifier.database.isDefined =>
                        // delay the exception into CheckAnalysis, then it could be resolved as data source.
                        u
                }
            case rr : RecursiveRelation =>
                try {
                        rr
                } catch {
                    case _: AnalysisException if rr.tableIdentifier.database.isDefined =>
                        // delay the exception into CheckAnalysis, then it could be resolved as data source.
                        rr
                }
//            case ct: CacheTableCommand if ct.plan.get.isInstanceOf[UnresolvedRelation] =>
//                try {
//                    val unresolvedRelation = ct.plan.get.asInstanceOf[UnresolvedRelation]
//                    val resolvedRelation = getTable(unresolvedRelation)
//                    CacheTableCommand(unresolvedRelation.tableName, Option(resolvedRelation), isLazy = true)
//                } catch {
//                    case _: AnalysisException if ct.plan.get.asInstanceOf[UnresolvedRelation].tableIdentifier.database.isDefined =>
//                        // delay the exception into CheckAnalysis, then it could be resolved as data source.
//                        ct.plan.get.asInstanceOf[UnresolvedRelation]
//                }

        }
    }
    object ResolveAliases2 extends Rule[LogicalPlan] {
        private def assignAliases(exprs: Seq[NamedExpression]) = {
            val p = exprs.zipWithIndex.map {
                case (expr, i) =>
                    expr transformUp {
                        case u @ UnresolvedAlias(child) => child match {
                            case ne: NamedExpression => ne
                            case e if !e.resolved => u
                            case g: Generator => MultiAlias(g, Nil)
                            case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)()
                            case other => Alias(other, s"_c$i")()
                        }
                    }
            }.asInstanceOf[Seq[NamedExpression]]
            p
        }

        private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
            exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

        def apply(plan: LogicalPlan): LogicalPlan = {
            val lp = plan resolveOperators {
                case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
                    Aggregate(groups, assignAliases(aggs), child)

                case MonotonicAggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
                    MonotonicAggregate(assignAliases(groups.asInstanceOf[Seq[NamedExpression]]), assignAliases(aggs), child)

                case g: GroupingAnalytics if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
                    g.withNewAggs(assignAliases(g.aggregations))

                case Pivot(groupByExprs, pivotColumn, pivotValues, aggregates, child)
                    if child.resolved && hasUnresolvedAlias(groupByExprs) =>
                    Pivot(assignAliases(groupByExprs), pivotColumn, pivotValues, aggregates, child)

                case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
                    Project(assignAliases(projectList), child)
            }
            lp
        }
    }


object CleanupAliases2 extends Rule[LogicalPlan] {
    private def trimAliases(e: Expression): Expression = {
        var stop = false
        e.transformDown {
            // CreateStruct is a special case, we need to retain its top level Aliases as they decide the
            // name of StructField. We also need to stop transform down this expression, or the Aliases
            // under CreateStruct will be mistakenly trimmed.
            case c: CreateStruct if !stop =>
                stop = true
                c.copy(children = c.children.map(trimNonTopLevelAliases))
            case c: CreateStructUnsafe if !stop =>
                stop = true
                c.copy(children = c.children.map(trimNonTopLevelAliases))
            case Alias(child, _) if !stop => child
        }
    }

    def trimNonTopLevelAliases(e: Expression): Expression = e match {
        case a: Alias =>
            Alias(trimAliases(a.child), a.name)(a.exprId, a.qualifiers, a.explicitMetadata)
        case other => trimAliases(other)
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
        val lp = plan resolveOperators {
            case Project(projectList, child) =>
                val cleanedProjectList =
                    projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
                Project(cleanedProjectList, child)

            case Aggregate(grouping, aggs, child) =>
                val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
                Aggregate(grouping.map(trimAliases), cleanedAggs, child)

            case MonotonicAggregate(grouping, aggs, child) =>
                val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
                MonotonicAggregate(grouping.map(trimAliases), cleanedAggs, child)

            case w@Window(projectList, windowExprs, partitionSpec, orderSpec, child) =>
                val cleanedWindowExprs =
                    windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
                Window(projectList, cleanedWindowExprs, partitionSpec.map(trimAliases),
                    orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

            case other =>
                var stop = false
                other transformExpressionsDown {
                    case c: CreateStruct if !stop =>
                        stop = true
                        c.copy(children = c.children.map(trimNonTopLevelAliases))
                    case c: CreateStructUnsafe if !stop =>
                        stop = true
                        c.copy(children = c.children.map(trimNonTopLevelAliases))
                    case Alias(child, _) if !stop => child
                }
            }
            lp
        }
    }

}
