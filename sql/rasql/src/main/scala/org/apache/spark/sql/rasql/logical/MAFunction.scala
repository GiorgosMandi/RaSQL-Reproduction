

package org.apache.spark.sql.rasql.logical

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, AttributeSet, Cast, Coalesce, Expression, Greatest, If, IsNull, Least, Literal, Or, Unevaluable}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType, LongType}


abstract class MonotonicAggregateFunction extends DeclarativeAggregate with Serializable {}

case class MMax(child: Expression) extends MonotonicAggregateFunction {

    override def children: Seq[Expression] = child :: Nil

    override def nullable: Boolean = true

    override def dataType: DataType = child.dataType

    override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

    override def checkInputDataTypes(): TypeCheckResult = TypeUtils.checkForOrderingExpr(child.dataType, caller="function mmax")

    private lazy val mmax = AttributeReference("mmax", child.dataType)()

    override lazy val aggBufferAttributes: Seq[AttributeReference] = mmax :: Nil

    override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, child.dataType))

    override lazy val updateExpressions: Seq[Expression] = Seq(Greatest(Seq(mmax, child)))

    override lazy val mergeExpressions: Seq[Expression] = Seq(Greatest(Seq(mmax.left, mmax.right)))

    override lazy val evaluateExpression: AttributeReference = mmax
}



case class MMin(child: Expression) extends MonotonicAggregateFunction {

    override def children: Seq[Expression] = child :: Nil

    override def nullable: Boolean = true

    override def dataType: DataType = child.dataType

    override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

    override def checkInputDataTypes(): TypeCheckResult = TypeUtils.checkForOrderingExpr(child.dataType, caller="function mmin")

    private lazy val mmin = AttributeReference("mmin", child.dataType)()

    override lazy val aggBufferAttributes: Seq[AttributeReference] = mmin :: Nil

    override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, child.dataType))

    override lazy val updateExpressions: Seq[Expression] = Seq(Least(Seq(mmin, child)))

    override lazy val mergeExpressions: Seq[Expression] = Seq(Least(Seq(mmin.left, mmin.right)))

    override lazy val evaluateExpression: AttributeReference = mmin
}

case class MSum(child: Expression) extends MonotonicAggregateFunction {

    override def children: Seq[Expression] = child :: Nil

    override def nullable: Boolean = true

    override def dataType: DataType = child.dataType

    override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

    override def checkInputDataTypes(): TypeCheckResult = TypeUtils.checkForOrderingExpr(child.dataType, caller="function msum")

    private lazy val msum = AttributeReference("msum", child.dataType)()

    private lazy val zero = Cast(Literal(0), child.dataType)

    override lazy val aggBufferAttributes: Seq[AttributeReference] = msum :: Nil

    override lazy val initialValues: Seq[Literal] = Seq(Literal.create(null, child.dataType))

    override lazy val updateExpressions: Seq[Expression] = // Seq(Add(msum, child))
        Seq(Coalesce(Seq(Add(Coalesce(Seq(msum, zero)), Cast(child, child.dataType)), msum)))

    override lazy val mergeExpressions: Seq[Expression] = { // Seq(Add(msum.left, msum.right))
        val add = Add(Coalesce(Seq(msum.left, zero)), Cast(msum.right, child.dataType))
        Seq(Coalesce(Seq(add, msum.left)))
    }

    override lazy val evaluateExpression: Expression = Cast(msum, child.dataType)
}


case class MCount(child: Expression) extends MonotonicAggregateFunction {

    override def children: Seq[Expression] = child :: Nil

    override def nullable: Boolean = false

    override def dataType: DataType = LongType

    override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(AnyDataType)

    private lazy val mcount = AttributeReference("mcount", LongType)()

    override lazy val aggBufferAttributes: Seq[AttributeReference] = mcount :: Nil

    override lazy val initialValues = Seq(Literal(0L))

    override lazy val updateExpressions = Seq(If(children.map(IsNull).reduce(Or), mcount, Add(mcount, Literal(1L))))

    override lazy val mergeExpressions = Seq( Add(mcount.left, mcount.right))

    override lazy val evaluateExpression: Expression = Cast(mcount, LongType)

    override def defaultResult: Option[Literal] = Option(Literal(0L))

}

case class MonotonicAggregateExpression(aggregateFunction: MonotonicAggregateFunction, mode: AggregateMode, isDistinct: Boolean)
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

    override def prettyString: String = aggregateFunction.prettyString

    override def toString: String = s"(${aggregateFunction},mode=$mode,isDistinct=$isDistinct)"
}