package org.apache.spark.sql.rasql.logical

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.{BooleanType, DataType}

case class PreMapFunction(pm: Expression, child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
}

case class RecursiveJoin(left: LogicalPlan, right: LogicalPlan, condition: Option[Expression]) extends BinaryNode {
    override def output: Seq[Attribute] =left.output ++ right.output

    def selfJoinResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

    override lazy val resolved: Boolean = {
        childrenResolved &&
            expressions.forall(_.resolved) &&
            selfJoinResolved &&
            condition.forall(_.dataType == BooleanType) // TODO try instead forall take and forall
    }
}


case class RecursiveAggregateExpr(aggregateFunction: AggregateFunction, mode: AggregateMode,
                                  fName: String, attrName: Expression)
    extends Expression with Unevaluable {

    def getFunctionName: String = fName

    def getTargetAttributeAlias: String = attrName.asInstanceOf[UnresolvedAttribute].name

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

case class MonotonicAggregate(groupingExpressions: Seq[Expression],
                              aggregateExpressions: Seq[NamedExpression],
                              child: LogicalPlan) extends UnaryNode {
    override lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

    override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}


case class RecursiveRelation(tableIdentifier: TableIdentifier) extends LeafNode {

    def tableName: String = tableIdentifier.unquotedString

    override def output: Seq[Attribute] = Nil
}


case class AggregateRecursion(name: String,
                              left: LogicalPlan,
                              right: LogicalPlan) extends BinaryNode {
    // left is exitRules plan
    // right is recursive rules plan
    override def output: Seq[Attribute] = right.output
}

