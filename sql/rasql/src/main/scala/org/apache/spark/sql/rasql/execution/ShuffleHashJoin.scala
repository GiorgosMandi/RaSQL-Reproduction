package org.apache.spark.sql.rasql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Performs a shuffle hash join
 *
 * @param leftKeys the aggregation key of left plan
 * @param rightKeys the aggregation key of right plan
 * @param buildSide which side will be build and cached (the other will be streamed)
 * @param left      left plan
 * @param right     right plan
 */
case class ShuffleHashJoin(leftKeys: Seq[Expression], rightKeys: Seq[Expression], buildSide: BuildSide, left: SparkPlan, right: SparkPlan)
    extends BinaryNode with HashJoin {

    @transient
    final protected val rasqlContext = SQLContext.getActive().orNull.asInstanceOf[RaSQLContext]

    override lazy val metrics = Map(
        "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
        "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
        "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

    var cachedBuildPlan: RDD[HashedRelation] = _

    override def output: Seq[Attribute] = left.output ++ right.output

    override def outputPartitioning: Partitioning = PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

    override def requiredChildDistribution: Seq[ClusteredDistribution] = ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

    override def outputsUnsafeRows: Boolean = true
    override def canProcessUnsafeRows: Boolean = true
    override def canProcessSafeRows: Boolean = false

    /**
     * Execution
     * @return a joined RDD
     */
    protected override def doExecute(): RDD[InternalRow] = {
        val numStreamedRows = buildSide match {
            case BuildLeft => longMetric("numRightRows")
            case BuildRight => longMetric("numLeftRows")
        }
        val numOutputRows = longMetric("numOutputRows")

        // build and cache the building plan - if it's already cached, don't execute
        if (cachedBuildPlan == null) {
            cachedBuildPlan = buildPlan.execute()
                .mapPartitionsInternal(iter => Iterator(HashedRelation(iter, SQLMetrics.nullLongMetric, buildSideKeyGenerator)), preservesPartitioning = true)
                .setName("cachedRelation")
                .persist(StorageLevel.MEMORY_AND_DISK)
        }

        // join two plans
        cachedBuildPlan.zipPartitions(streamedPlan.execute()) { (buildIter, streamedIter) => hashJoin(streamedIter, numStreamedRows, buildIter.next(), numOutputRows)}
    }
}
