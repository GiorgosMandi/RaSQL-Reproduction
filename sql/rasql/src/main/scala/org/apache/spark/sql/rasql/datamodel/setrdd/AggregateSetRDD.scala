

package org.apache.spark.sql.rasql.datamodel.setrdd

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.TungstenMonotonicAggregationIterator
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.sql.rasql.execution.MonotonicAggregate
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class AggregateSetRDD(val partitionsRDD: RDD[AggregateSetRDDPartition], monotonicAggregate: MonotonicAggregate)
    extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

    protected def self: AggregateSetRDD = this

    setName("AggregateSetRDD")

    @transient
    final val rasqlContext: RaSQLContext = SQLContext.getActive().get.asInstanceOf[RaSQLContext]

    override val partitioner: Option[Partitioner] = Option(partitionsRDD.partitioner.getOrElse(rasqlContext.hashPartitioner))

    override protected def getPreferredLocations(s: Partition): Seq[String] =
        partitionsRDD.preferredLocations(s)

    override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

    override def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): this.type = {
        partitionsRDD.persist(newLevel)
        this
    }

    override def unpersist(blocking: Boolean = true): this.type = {
        partitionsRDD.unpersist(blocking)
        this
    }

    override def cache(): this.type = this.persist()

    override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

    override def checkpoint(): Unit = {
        partitionsRDD.checkpoint()
    }

    override def isCheckpointed: Boolean = {
        firstParent[AggregateSetRDDPartition].isCheckpointed
    }

    override def getCheckpointFile: Option[String] = {
        partitionsRDD.getCheckpointFile
    }

    override def localCheckpoint(): AggregateSetRDD.this.type = {
        partitionsRDD.localCheckpoint()
        this
    }

    override def memoryCheckpoint(): Unit = {
        partitionsRDD.memoryCheckpoint()
        this
    }

    override def mapPartitions[U: ClassTag](f: Iterator[InternalRow] => Iterator[U], preservesPartitioning: Boolean = true): RDD[U] = {
        partitionsRDD.mapPartitions(iter => f(iter.next().iterator), preservesPartitioning)
    }

    override def mapPartitionsInternal[U: ClassTag](f: Iterator[InternalRow] => Iterator[U], preservesPartitioning: Boolean = true): RDD[U] = {
        partitionsRDD.mapPartitionsInternal(iter => f(iter.next().iterator), preservesPartitioning)
    }

    override def setName(_name: String): this.type = {
        name = _name
        partitionsRDD.setName(_name)
        this
    }

    override def count(): Long = partitionsRDD.map(_.size).reduce(_ + _)

    override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        if (isCheckpointed)
            firstParent[AggregateSetRDDPartition].iterator(split, context).next.iterator
        else
            compute(split, context)
    }

    override def compute(part: Partition, context: TaskContext): Iterator[InternalRow] = {
        firstParent[AggregateSetRDDPartition].iterator(part, context).next.iterator
    }

    // assume other is already partitioned by AggrGroup
    def update(other: RDD[InternalRow]): (AggregateSetRDD, SetRDD) = {
        val newPartitionsRDD = partitionsRDD.zipPartitions(other, preservesPartitioning = true) { (thisIter, otherIter) =>
            val thisPart = thisIter.next()
            Iterator(thisPart.update(otherIter, monotonicAggregate))
        }

        val allSetRDD: RDD[AggregateSetRDDPartition] = newPartitionsRDD
            .mapPartitionsInternal(_.map(_._1), preservesPartitioning = true)

        val deltaSPrimeRDD: RDD[HashSetPartition] = newPartitionsRDD
            .mapPartitionsInternal(_.map(_._2), preservesPartitioning = true)

        (new AggregateSetRDD(allSetRDD, monotonicAggregate).setName("AggregateSetRDD - all"),
            new SetRDD(deltaSPrimeRDD).setName("SetRDD - deltaSPrime"))
    }
}

object AggregateSetRDD extends Logging {
    // this expects that the rows are already shuffled by AggrGroup which is the case when called from MonotonicAggregate
    def apply(rdd: RDD[InternalRow], schema: StructType, monotonicAggregate: MonotonicAggregate): AggregateSetRDD = {

        val numInputRows = monotonicAggregate.longMetric("numInputRows")
        val numOutputRows = monotonicAggregate.longMetric("numOutputRows")
        val dataSize = monotonicAggregate.longMetric("dataSize")
        val spillSize = monotonicAggregate.longMetric("spillSize")

        val isPrem = monotonicAggregate.isMin || monotonicAggregate.isMax || monotonicAggregate.isSum || monotonicAggregate.isCount

        val aggregateSetRDDPartitions: RDD[AggregateSetRDDPartition] = if (isPrem) {
            val keyPositions = Array.fill(schema.length)(1)
            // last column is aggregate value
            keyPositions(schema.length - 1) = 0

            // likely the iterator is produced from a shuffle or base relation
            val output = rdd.mapPartitionsInternal(iter => {
                val taIter = new TungstenMonotonicAggregationIterator(
                    monotonicAggregate.groupingExpressions,
                    monotonicAggregate.nonCompleteAggregateExpressions,
                    monotonicAggregate.nonCompleteAggregateAttributes,
                    monotonicAggregate.completeAggregateExpressions,
                    monotonicAggregate.completeAggregateAttributes,
                    monotonicAggregate.initialInputBufferOffset,
                    monotonicAggregate.resultExpressions,
                    monotonicAggregate.newMutableProjection,
                    monotonicAggregate.child.output,
                    iter,
                    monotonicAggregate.testFallbackStartsAt,
                    numInputRows,
                    numOutputRows,
                    dataSize,
                    spillSize,
                    null)

                Iterator(new AggregateSetRDDPartition(taIter.hashMap, monotonicAggregate, schema.length))
            }, preservesPartitioning = true)

            output.asInstanceOf[RDD[AggregateSetRDDPartition]]
        } else {
            throw new Exception()
        }

        new AggregateSetRDD(aggregateSetRDDPartitions, monotonicAggregate)
    }
}