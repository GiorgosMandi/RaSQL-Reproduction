package org.apache.spark.sql.rasql

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Final, Partial}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}
import org.apache.spark.sql.rasql.execution.MonotonicAggregate

class RaSQLPlanner(val rc: RaSQLContext) extends SparkPlanner(rc) {
    self: RaSQLPlanner =>

    override def strategies: Seq[Strategy] = (RecursiveAggregation :: MonotonicAggregation :: Nil) ++ super.strategies

    object RecursiveAggregation extends Strategy {
        def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
            case logical.AggregateRecursion(name, left, right) =>
                execution.AggregateRecursion(name, planLater(left), planLater(right)) :: Nil
            case logical.RecursiveRelation(table, output) =>
                execution.RecursiveRelation(table.unquotedString, output) :: Nil
            case _ => Nil
        }
    }


    object MonotonicAggregation extends Strategy {

        def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

            case logical.MonotonicAggregate(groupingExpressions, resultExpressions, child) => {

                /* A single aggregate expression might appear multiple times in resultExpressions.
                   In order to avoid evaluating an individual aggregate function multiple times, we'll
                   build a set of the distinct aggregate expressions and build a function which can
                   be used to re-write expressions so that they reference the single copy of the
                   aggregate function which actually gets computed. */

                val aggregateExpressions = resultExpressions.flatMap { expr =>
                    expr.collect {
                        case agg: AggregateExpression => agg
                    }
                }.distinct

                /* For those distinct aggregate expressions, we create a map from the
                   aggregate function to the corresponding attribute of the function. */

                val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
                    val aggregateFunction = agg.aggregateFunction
                    val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
                    (aggregateFunction, agg.isDistinct) -> attribute
                }.toMap

                val (functionsWithDistinct, functionsWithoutDistinct) = aggregateExpressions.partition(_.isDistinct)
                if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
                    /* This is a sanity check. We should not reach here when we have multiple distinct
                       column sets. Our MultipleDistinctRewriter should take care this case. */
                    sys.error("You hit a query analyzer bug. Please report your query to " +
                        "Spark user mailing list.")
                }

                val namedGroupingExpressions = groupingExpressions.map {
                    case ne: NamedExpression => ne -> ne
                    /* If the expression is not a NamedExpressions, we add an alias.
                       So, when we generate the result of the operator, the Aggregate Operator
                       can directly get the Seq of attributes representing the grouping expressions. */
                    case other =>
                        val withAlias = Alias(other, other.toString)()
                        other -> withAlias
                }
                val groupExpressionMap = namedGroupingExpressions.toMap

                /* The original `resultExpressions` are a set of expressions which may reference
                   aggregate expressions, grouping column values, and constants. When aggregate operator
                   emits output rows, we will use `resultExpressions` to generate an output projection
                   which takes the grouping columns and final aggregate result buffer as input.
                   Thus, we must re-write the result expressions so that their attributes match up with
                   the attributes of the final result projection's input row: */
                val rewrittenResultExpressions = resultExpressions.map { expr =>
                    expr.transformDown {
                        case AggregateExpression(aggregateFunction, _, isDistinct) =>
                            /* The final aggregation buffer's attributes will be `finalAggregationAttributes`,
                               so replace each aggregate expression by its corresponding attribute in the set: */
                            aggregateFunctionToAttribute(aggregateFunction, isDistinct)
                        case expression =>
                            /* Since we're using `namedGroupingAttributes` to extract the grouping key
                               columns, we need to replace grouping key expressions with their corresponding
                               attributes. We do not rely on the equality check at here since attributes may
                              differ cosmetically. Instead, we use semanticEquals. */
                            groupExpressionMap.collectFirst {
                                case (expr, ne) if expr semanticEquals expression => ne.toAttribute
                            }.getOrElse(expression)
                    }.asInstanceOf[NamedExpression]
                }

                planMonotonicAggregate(namedGroupingExpressions.map(_._2), aggregateExpressions, aggregateFunctionToAttribute,
                                        rewrittenResultExpressions, planLater(child))
            }
            case _ => Nil
        }
    }


    def planMonotonicAggregate(groupingExpressions: Seq[NamedExpression],
                               aggregateExpressions: Seq[AggregateExpression],
                               aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
                               resultExpressions: Seq[NamedExpression],
                               child: SparkPlan): Seq[SparkPlan] = {

        // TODO before it was doing a partial and the a total - now I have changed it
        // MonotonicAggregation will do partial aggregation

        val groupingAttributes = groupingExpressions.map(_.toAttribute)
        val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))
        val partialAggregateAttributes =
            partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
        val partialResultExpressions =
            groupingAttributes ++
                partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

        val partialAggregate = MonotonicAggregate(
            requiredChildDistributionExpressions = None: Option[Seq[Expression]],
            groupingExpressions = groupingExpressions,
            nonCompleteAggregateExpressions = partialAggregateExpressions,
            nonCompleteAggregateAttributes = partialAggregateAttributes,
            completeAggregateExpressions = Nil,
            completeAggregateAttributes = Nil,
            initialInputBufferOffset = 0,
            resultExpressions = partialResultExpressions,
            child = child)

        // 2. Create an Aggregate Operator for final aggregations.
        val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
        // The attributes of the final aggregation buffer, which is presented as input to the result
        // projection:
        val finalAggregateAttributes = finalAggregateExpressions.map {
            expr => aggregateFunctionToAttribute(expr.aggregateFunction, expr.isDistinct)
        }

        val finalAggregate = MonotonicAggregate(
            requiredChildDistributionExpressions = Some(groupingAttributes),
            groupingExpressions = groupingAttributes,
            nonCompleteAggregateExpressions = finalAggregateExpressions,
            nonCompleteAggregateAttributes = finalAggregateAttributes,
            completeAggregateExpressions = Nil,
            completeAggregateAttributes = Nil,
            initialInputBufferOffset = groupingExpressions.length,
            resultExpressions = resultExpressions,
            child = partialAggregate)

        finalAggregate :: Nil
    }
}
