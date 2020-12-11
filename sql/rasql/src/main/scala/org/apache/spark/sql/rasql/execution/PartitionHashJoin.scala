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

/* We have resurrected this class from earlier versions of Spark because for
* recursion, caching the build-side of the join and reusing it each iteration is likely
* better than performing a sort-merge-join each iteration.
* TODO - take a look at optimizing sort-merge-join for recursive use.
*/
case class PartitionHashJoin(leftKeys: Seq[Expression], rightKeys: Seq[Expression], buildSide: BuildSide, left: SparkPlan, right: SparkPlan)
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

    protected override def doExecute(): RDD[InternalRow] = {
        val numStreamedRows = buildSide match {
            case BuildLeft => longMetric("numRightRows")
            case BuildRight => longMetric("numLeftRows")
        }
        val numOutputRows = longMetric("numOutputRows")

        if (cachedBuildPlan == null) {
            cachedBuildPlan = buildPlan.execute()
                .mapPartitionsInternal(iter => Iterator(HashedRelation(iter, SQLMetrics.nullLongMetric, buildSideKeyGenerator)), preservesPartitioning = true)
                .setName("cachedRelation")
                .persist(StorageLevel.MEMORY_AND_DISK)
        }
        val joined = cachedBuildPlan.zipPartitions(streamedPlan.execute()) { (buildIter, streamedIter) => hashJoin(streamedIter, numStreamedRows, buildIter.next(), numOutputRows)}
        joined
    }
}
