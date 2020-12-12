
package org.apache.spark.sql.rasql.datamodel.setrdd

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rasql.datamodel.internalset.InnerIterator
import org.apache.spark.sql.rasql.{PreMapFunction, RaSQLContext}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag


/**
 * SetRDD, this structure is lended from BigDatalog
 *
 * It's an RDD that its partitions consist of a Single HashMap (InnerHashMap), and it's used for
 * union and differentiation of co-partitioned Set RDDs
 *
 * @param partitionsRDD partitionsRDD
 */
class SetRDD(var partitionsRDD: RDD[HashMapPartition]) extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

    protected def self: SetRDD = this

    setName("SetRDD")

    override val partitioner: Option[Partitioner] = partitionsRDD.partitioner

    @transient
    final val rasqlContext: RaSQLContext = SQLContext.getActive().get.asInstanceOf[RaSQLContext]

    /** MAIN FUNCTIONS */

    /**
     *  Differentiation:
     *      Differentiates the corresponding partitions of the two RDDs
      * @param other other RDD
     * @return SetRDD with differentiated partitions
     */
    def diff(other: RDD[InternalRow]): SetRDD = {
        val diffRDD = other match {
            case other: SetRDD =>
                this.zipSetRDDPartitions(other)((thisIter, otherIter) => {
                    val thisPart = thisIter.next()
                    val otherPart = otherIter.next()
                    Iterator(thisPart.diff(otherPart.iterator, id))
                })
            case _ =>
                this.zipPartitionsWithOther(other)((thisIter, otherIter) => {
                    val thisPart = thisIter.next()
                    Iterator(thisPart.diff(otherIter, id))
                })
        }
        diffRDD.setName("SetRDD.diffRDD")
    }

    /**
     * Union:
     *      Unions the corresponing partitions of the two RDDs
     * @param other other RDD
     * @return SetRDD with unioned partitions
     */
    override def union(other: RDD[InternalRow]): SetRDD = {
        val unionRDD = other match {
            case other: SetRDD  =>
                this.zipSetRDDPartitions(other)((thisIter, otherIter) => {
                    val thisPart = thisIter.next()
                    val otherPart = otherIter.next()
                    Iterator(thisPart.union(otherPart, id))
                })
            case _ =>
                this.zipPartitionsWithOther(other)((thisIter, otherIter) => {
                    val thisPart = thisIter.next()
                    Iterator(thisPart.union(otherIter, id))
                })
        }
        unionRDD.setName("SetRDD.unionRDD")
    }


    /** OVERRIDED RDD OPERATIONS */
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

    override def collect(): Array[InternalRow] = partitionsRDD.flatMap(hp => (new InnerIterator(hp.map)).get()).collect()

    override def cache(): this.type = this.persist()

    override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

    override def checkpoint(): Unit = {
        partitionsRDD.checkpoint()
    }

    override def isCheckpointed: Boolean = {
        firstParent[HashMapPartition].isCheckpointed
    }

    override def getCheckpointFile: Option[String] = {
        partitionsRDD.getCheckpointFile
    }

    override def localCheckpoint(): SetRDD.this.type = {
        partitionsRDD.localCheckpoint()
        this
    }

    override def memoryCheckpoint(): Unit = {
        partitionsRDD.memoryCheckpoint()
    }

    override def mapPartitions[U: ClassTag](f: Iterator[InternalRow] => Iterator[U], preservesPartitioning: Boolean = true): RDD[U] =
        partitionsRDD.mapPartitions(iter => f(iter.next().iterator), preservesPartitioning)

    override def mapPartitionsInternal[U: ClassTag](f: Iterator[InternalRow] => Iterator[U], preservesPartitioning: Boolean = true): RDD[U] =
        partitionsRDD.mapPartitionsInternal(iter => f(iter.next().iterator), preservesPartitioning)

    override def setName(_name: String): this.type = {
        name = _name
        partitionsRDD.setName(_name)
        this
    }

    override def count(): Long = partitionsRDD.map(_.size).reduce(_ + _)

    override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        if (isCheckpointed)
            firstParent[HashMapPartition].iterator(split, context).next.iterator
        else
            compute(split, context)
    }

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
        firstParent[HashMapPartition].iterator(split, context).next.iterator

    private def zipSetRDDPartitions(other: SetRDD)
                                   (f: (Iterator[HashMapPartition], Iterator[HashMapPartition])
                                       => Iterator[HashMapPartition]): SetRDD = {
        val rdd = partitionsRDD.zipPartitions(other.partitionsRDD, preservesPartitioning = true)(f)
        new SetRDD(rdd)
    }

    private def zipPartitionsWithOther(other: RDD[InternalRow])
                                      (f: (Iterator[HashMapPartition], Iterator[InternalRow])
                                          => Iterator[HashMapPartition]): SetRDD = {
        val rdd = partitionsRDD.zipPartitions(other, preservesPartitioning = true)(f)
        new SetRDD(rdd)
    }
}

/**
 * Auxiliary Constructor
 */
object SetRDD {
    def apply(rdd: RDD[InternalRow], pf: PreMapFunction): SetRDD = {
        val setRDDPartitions = rdd.mapPartitionsInternal[HashMapPartition] (iter => Iterator(HashMapPartition(iter, pf)), preservesPartitioning = true)
        new SetRDD(setRDDPartitions)
    }
}