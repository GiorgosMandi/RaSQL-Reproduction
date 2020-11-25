package org.apache.spark.sql.rasql.execution



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateMode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.aggregate.TungstenAggregationIterator
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.sql.types.StructType

case class MonotonicAggregatePartial(requiredChildDistributionExpressions: Option[Seq[Expression]],
                                     groupingExpressions: Seq[NamedExpression],
                                     nonCompleteAggregateExpressions: Seq[AggregateExpression],
                                     nonCompleteAggregateAttributes: Seq[Attribute],
                                     completeAggregateExpressions: Seq[AggregateExpression],
                                     completeAggregateAttributes: Seq[Attribute],
                                     initialInputBufferOffset: Int,
                                     resultExpressions: Seq[NamedExpression],
                                     child: SparkPlan)
    extends UnaryNode {

    @transient
    final val rasqlContext = SQLContext.getActive().orNull.asInstanceOf[RaSQLContext]


    val aggregateBufferAttributes: Seq[AttributeReference] = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).flatMap(_.aggregateFunction.aggBufferAttributes)


    require(MonotonicAggregatePartial.supportsAggregate(groupingExpressions, aggregateBufferAttributes))

    override lazy val metrics = Map(
        "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
        "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
        "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
        "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
        "numUpdates" -> SQLMetrics.createLongMetric(sparkContext, "number of updates"))

    override def outputsUnsafeRows: Boolean = true

    override def canProcessUnsafeRows: Boolean = true

    override def canProcessSafeRows: Boolean = true

    override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

    override def requiredChildDistribution: List[Distribution] = {
        UnspecifiedDistribution :: Nil
    }

    var aggregationMode: (Option[AggregateMode], Option[AggregateMode]) = {
        nonCompleteAggregateExpressions.map(_.mode).distinct.headOption ->
            completeAggregateExpressions.map(_.mode).distinct.headOption
    }

    // This is for testing. We force TungstenAggregationIterator to fall back to sort-based
    // aggregation once it has processed a given number of input rows.
    //private
    val testFallbackStartsAt: Option[Int] = {
        sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
            case null | "" => None
            case fallbackStartsAt => Some(fallbackStartsAt.toInt)
        }
    }

    protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
        val numInputRows = longMetric("numInputRows")
        val numOutputRows = longMetric("numOutputRows")
        val dataSize = longMetric("dataSize")
        val spillSize = longMetric("spillSize")

        val rdd: RDD[InternalRow] = child.execute().mapPartitions { iter =>
            val hasInput = iter.hasNext
            if (!hasInput && groupingExpressions.nonEmpty) {
                // This is a grouped aggregate and the input iterator is empty,
                // so return an empty iterator.
                Iterator.empty
            } else {
                val aggregationIterator = new TungstenAggregationIterator(
                    groupingExpressions,
                    nonCompleteAggregateExpressions,
                    nonCompleteAggregateAttributes,
                    completeAggregateExpressions,
                    completeAggregateAttributes,
                    initialInputBufferOffset,
                    resultExpressions,
                    newMutableProjection,
                    child.output,
                    iter,
                    testFallbackStartsAt,
                    numInputRows,
                    numOutputRows,
                    dataSize,
                    spillSize)

                if (!hasInput && groupingExpressions.isEmpty) {
                    val numOutputRows = longMetric("numOutputRows")
                    numOutputRows += 1
                    Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
                } else {
                    aggregationIterator
                }
            }
        }
        rdd
    }

    override def simpleString: String = {
        val allAggregateExpressions = nonCompleteAggregateExpressions ++ completeAggregateExpressions

        testFallbackStartsAt match {
            case None =>
                val keyString = groupingExpressions.mkString("[", ",", "]")
                val functionString = allAggregateExpressions.mkString("[", ",", "]")
                val outputString = output.mkString("[", ",", "]")
                s"MonotonicAggregatePartial(key=$keyString, functions=$functionString, output=$outputString)"
            case Some(fallbackStartsAt) =>
                s"MonotonicAggregateWithControlledFallback $groupingExpressions " +
                    s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
        }
    }
}

object MonotonicAggregatePartial {
    def supportsAggregate(groupingExpressions: Seq[Expression],
                          aggregateBufferAttributes: Seq[Attribute]): Boolean = {
        val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
        UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
    }
}
