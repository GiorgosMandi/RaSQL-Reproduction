package org.apache.spark.sql.rasql.logical

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.DataType


sealed trait MonotonicAggregate extends UnaryNode {
    val groupingExpressions: Seq[Expression]
    val aggregateExpressions: Seq[NamedExpression]
    val child: LogicalPlan

    override lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

    override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}

case class MonotonicAggregateGlobal(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan) extends MonotonicAggregate

case class MonotonicAggregatePartial(groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan) extends MonotonicAggregate

case class RecursiveRelation(tableIdentifier: TableIdentifier, var output: Seq[Attribute]) extends LeafNode {

    def tableName: String = tableIdentifier.unquotedString

    override def statistics: Statistics = Statistics(Long.MaxValue)
}


case class RecursiveAggregate(name: String, left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
    // left is exitRules plan
    // right is recursive rules plan
    override def output: Seq[Attribute] = right.output
}

/**
 * A hint for the optimizer that we should cache the `child` if used in a join operator.
 */
case class CacheHint(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
}


case class PreMapFunction(pm: Expression, child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
}

case class RecursiveAggregateExpr(aggregateFunction: AggregateFunction, mode: AggregateMode,
                                  fName: String, attrName: Expression)
    extends Expression with Unevaluable {

    def getFunctionName: String = fName

    def getTargetAttributeAlias: UnresolvedAttribute = attrName.asInstanceOf[UnresolvedAttribute]

    override def children: Seq[Expression] = aggregateFunction :: Nil

    override def dataType: DataType = aggregateFunction.dataType

    override def foldable: Boolean = false

    override def nullable: Boolean = aggregateFunction.nullable

    override def references: AttributeSet = {
        val childReferences = mode match {
            case Partial | Complete => aggregateFunction.references.toSeq
            case PartialMerge | Final => aggregateFunction.aggBufferAttributes
        }
        AttributeSet(childReferences)
    }
}