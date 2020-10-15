package org.apache.spark.sql.rasql

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode}
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


case class RecursiveAggregateExpr(aggregateFunction: AggregateFunction, mode: AggregateMode, isDistinct: Boolean)
    extends Expression with Unevaluable {

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


