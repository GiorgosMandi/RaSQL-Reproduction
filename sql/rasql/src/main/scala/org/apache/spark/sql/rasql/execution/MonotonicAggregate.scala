package org.apache.spark.sql.rasql.execution

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.TungstenMonotonicAggregationIterator
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMonotonicAggregationMap
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BindReferences, Expression, JoinedRow, NamedExpression, SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.rasql.datamodel.setrdd.AggregateSetRDD
import org.apache.spark.sql.rasql.{RaSQLContext, logical}
import org.apache.spark.sql.types.StructType

case class MonotonicAggregate(requiredChildDistributionExpressions: Option[Seq[Expression]],
                         groupingExpressions: Seq[NamedExpression],
                         nonCompleteAggregateExpressions: Seq[AggregateExpression],
                         nonCompleteAggregateAttributes: Seq[Attribute],
                         completeAggregateExpressions: Seq[AggregateExpression],
                         completeAggregateAttributes: Seq[Attribute],
                         initialInputBufferOffset: Int,
                         resultExpressions: Seq[NamedExpression],
                         child: SparkPlan) extends UnaryNode {


    @transient
    final val rasqlContext = SQLContext.getActive().orNull.asInstanceOf[RaSQLContext]

    @transient
    var allRDD: AggregateSetRDD = _

    override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

    val aggregateBufferAttributes: Seq[AttributeReference] = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).flatMap(_.aggregateFunction.aggBufferAttributes)

    override def outputsUnsafeRows: Boolean = true

    override def canProcessUnsafeRows: Boolean = true

    override def canProcessSafeRows: Boolean = true

    override def productIterator: Iterator[Any] =
        Iterator(requiredChildDistributionExpressions, groupingExpressions,nonCompleteAggregateExpressions,
            nonCompleteAggregateAttributes, completeAggregateExpressions, completeAggregateAttributes,
            initialInputBufferOffset, resultExpressions, child)

    override def requiredChildDistribution: List[Distribution] = {
        if (groupingExpressions == Nil)
            AllTuples :: Nil
        else
            ClusteredDistribution(groupingExpressions) :: Nil
    }

    def isMin: Boolean = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[logical.MMin])
    def isMax: Boolean = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[logical.MMax])
    def isSum: Boolean = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[logical.MSum])
    def isCount: Boolean = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[logical.MCount])

    override def outputPartitioning: Partitioning = {
        val numPartitions = rasqlContext.conf.numShufflePartitions
        UnknownPartitioning(numPartitions)
    }

    def execute(_allRDD: AggregateSetRDD): RDD[InternalRow] = {
        allRDD = _allRDD
        doExecute()
    }

    protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {

        if (allRDD == null) {
            if (!child.outputPartitioning.satisfies(this.requiredChildDistribution.head))
                throw new SparkException("There is a missing exchange operator which should have repartitioned the input rdd!")

            allRDD = AggregateSetRDD(child.execute(), schema, this)
            allRDD
        }
        else {
            val (temp1, _) = allRDD.update(child.execute())
            temp1.asInstanceOf[AggregateSetRDD]
        }
    }



    val aggregationMode: (Option[AggregateMode], Option[AggregateMode]) =
        nonCompleteAggregateExpressions.map(_.mode).distinct.headOption -> completeAggregateExpressions.map(_.mode).distinct.headOption

    def getAggregationIterator(iter: Iterator[InternalRow],
                               aggregateStore: UnsafeFixedWidthMonotonicAggregationMap): TungstenMonotonicAggregationIterator = {
        val numInputRows = longMetric("numInputRows")
        val numOutputRows = longMetric("numOutputRows")
        val dataSize = longMetric("dataSize")
        val spillSize = longMetric("spillSize")

        new TungstenMonotonicAggregationIterator(
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
            spillSize,
            aggregateStore)
    }


    def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {
        val allAggregateExpressions: Seq[AggregateExpression] = nonCompleteAggregateExpressions ++ completeAggregateExpressions

        def initializeAllAggregateFunctions(startingInputBufferOffset: Int): Array[AggregateFunction] = {
            var mutableBufferOffset = 0
            var inputBufferOffset: Int = startingInputBufferOffset
            val functions = new Array[AggregateFunction](allAggregateExpressions.length)
            var i = 0
            while (i < allAggregateExpressions.length) {
                val func = allAggregateExpressions(i).aggregateFunction
                val aggregateExpressionIsNonComplete = i < nonCompleteAggregateExpressions.length
                // We need to use this mode instead of func.mode in order to handle aggregation mode switching
                // when switching to sort-based aggregation:
                val mode = if (aggregateExpressionIsNonComplete) aggregationMode._1 else aggregationMode._2
                val funcWithBoundReferences = mode match {
                    case Some(Partial) | Some(Complete) if func.isInstanceOf[ImperativeAggregate] =>
                        // We need to create BoundReferences if the function is not an
                        // expression-based aggregate function (it does not support code-gen) and the mode of
                        // this function is Partial or Complete because we will call eval of this
                        // function's children in the update method of this aggregate function.
                        // Those eval calls require BoundReferences to work.
                        BindReferences.bindReference(func, child.output)
                    case _ =>
                        // We only need to set inputBufferOffset for aggregate functions with mode
                        // PartialMerge and Final.
                        val updatedFunc = func match {
                            case function: ImperativeAggregate =>
                                function.withNewInputAggBufferOffset(inputBufferOffset)
                            case function => function
                        }
                        inputBufferOffset += func.aggBufferSchema.length
                        updatedFunc
                }
                val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
                    case function: ImperativeAggregate =>
                        // Set mutableBufferOffset for this function. It is important that setting
                        // mutableBufferOffset happens after all potential bindReference operations
                        // because bindReference will create a new instance of the function.
                        function.withNewMutableAggBufferOffset(mutableBufferOffset)
                    case function => function
                }
                mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
                functions(i) = funcWithUpdatedAggBufferOffset
                i += 1
            }
            functions
        }

        val allAggregateFunctions: Array[AggregateFunction] = initializeAllAggregateFunctions(initialInputBufferOffset)

        val groupingAttributes = groupingExpressions.map(_.toAttribute)
        val bufferAttributes = allAggregateFunctions.flatMap(_.aggBufferAttributes)

        aggregationMode match {
            // Partial-only or PartialMerge-only: every output row is basically the values of
            // the grouping expressions and the corresponding aggregation buffer.
            case (Some(Partial), None) | (Some(PartialMerge), None) =>
                val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
                val bufferSchema = StructType.fromAttributes(bufferAttributes)
                val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

                (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
                    unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
                }

            // Final-only, Complete-only and Final-Complete: a output row is generated based on
            // resultExpressions.
            case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
                val joinedRow = new JoinedRow()
                val evalExpressions = allAggregateFunctions.map {
                    case ae: DeclarativeAggregate => ae.evaluateExpression
                    case agg: AggregateFunction => NoOp
                }
                val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
                // These are the attributes of the row produced by `expressionAggEvalProjection`
                val aggregateResultSchema = nonCompleteAggregateAttributes ++ completeAggregateAttributes
                val aggregateResult = new SpecificMutableRow(aggregateResultSchema.map(_.dataType))
                expressionAggEvalProjection.target(aggregateResult)
                val resultProjection =
                    UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateResultSchema)

                (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
                    // Generate results for all expression-based aggregate functions.
                    expressionAggEvalProjection(currentBuffer)
                    // Generate results for all imperative aggregate functions.
                    resultProjection(joinedRow(currentGroupingKey, aggregateResult))
                }
        }
    }

    override lazy val metrics = Map(
        "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
        "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
        "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
        "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
        "numUpdates" -> SQLMetrics.createLongMetric(sparkContext, "number of updates")
    )

    /**
     * This is for testing. The TungstenAggregationIterator is forced to fall back to sort-based
     * aggregation once it has processed a given number of input rows.
     */
    val testFallbackStartsAt: Option[Int] = {
        sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
            case null | "" => None
            case fallbackStartsAt => Some(fallbackStartsAt.toInt)
        }
    }

    override def simpleString: String = {
        val allAggregateExpressions = nonCompleteAggregateExpressions ++ completeAggregateExpressions

        testFallbackStartsAt match {
            case None =>
                val keyString = groupingExpressions.mkString("[", ",", "]")
                val functionString = allAggregateExpressions.mkString("[", ",", "]")
                val outputString = output.mkString("[", ",", "]")
                s"MonotonicAggregate(key=$keyString, functions=$functionString, output=$outputString)"
            case Some(fallbackStartsAt) =>
                s"MonotonicAggregateWithControlledFallback $groupingExpressions " +
                    s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
        }
    }
}
