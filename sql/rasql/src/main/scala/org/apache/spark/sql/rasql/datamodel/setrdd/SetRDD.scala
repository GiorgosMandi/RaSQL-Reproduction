
package org.apache.spark.sql.rasql.datamodel.setrdd

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class SetRDD(var partitionsRDD: RDD[HashSetPartition]) extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

    protected def self: SetRDD = this

    setName("SetRDD")

    override val partitioner: Option[Partitioner] = partitionsRDD.partitioner

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
        firstParent[HashSetPartition].isCheckpointed
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
            firstParent[HashSetPartition].iterator(split, context).next.iterator
        else
            compute(split, context)
    }

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
        firstParent[HashSetPartition].iterator(split, context).next.iterator

    def diff(other: RDD[InternalRow]): SetRDD = {
        val diffRDD = other match {
            case other: SetRDD if partitioner == other.partitioner =>
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

    override def union(other: RDD[InternalRow]): SetRDD = {
        val unionRDD = other match {
            case other: SetRDD if partitioner == other.partitioner =>
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

    private def zipSetRDDPartitions(other: SetRDD)
                                   (f: (Iterator[HashSetPartition], Iterator[HashSetPartition])
                                       => Iterator[HashSetPartition]): SetRDD = {
        val rdd = partitionsRDD.zipPartitions(other.partitionsRDD, preservesPartitioning = true)(f)
        new SetRDD(rdd)
    }

    private def zipPartitionsWithOther(other: RDD[InternalRow])
                                      (f: (Iterator[HashSetPartition], Iterator[InternalRow])
                                          => Iterator[HashSetPartition]): SetRDD = {
        val rdd = partitionsRDD.zipPartitions(other, preservesPartitioning = true)(f)
        new SetRDD(rdd)
    }
}

object SetRDD {
    def apply(rdd: RDD[InternalRow], schema: StructType): SetRDD = {
        val setRDDPartitions = rdd.mapPartitionsInternal[HashSetPartition] (iter => Iterator(HashSetPartition(iter, schema.length)), preservesPartitioning = true)
        new SetRDD(setRDDPartitions)
    }
}