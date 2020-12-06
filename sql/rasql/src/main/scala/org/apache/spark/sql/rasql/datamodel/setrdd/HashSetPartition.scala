package org.apache.spark.sql.rasql.datamodel.setrdd

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rasql.datamodel.internalset.{InternalSet, SetIterator, TupleSet, IntSet}

import scala.reflect.ClassTag

class HashSetPartition(val set: InternalSet, numFactsGenerated: Long = 0, numFactsDerived: Long = 0, numFields: Int)(implicit val cTag: ClassTag[InternalRow])
    extends Serializable with org.apache.spark.Logging {

    def this(set: InternalSet, numFields: Int) = this(set, 0, 0, numFields)

    def size: Long = set.size()

    def iterator: Iterator[InternalRow] = new SetIterator(set)

    def union(otherPart: HashSetPartition, rddId: Int): HashSetPartition = {
        val newPartition = otherPart match {
            case otherPart: HashSetPartition => {
                set.union(otherPart.set)
                new HashSetPartition(set, numFields)
            }
            case _ => union(otherPart.iterator, rddId)
        }
        newPartition
    }


    def union(iter: Iterator[InternalRow], rddId: Int): HashSetPartition = {
        val newSet = set
        iter.foreach(next => newSet.insert(next))
        new HashSetPartition(newSet, numFields)
    }


    def diff(iter: Iterator[InternalRow], rddId: Int): HashSetPartition = {
        val diffSet: InternalSet = if (numFields == 2) new TupleSet() else new IntSet()
        var numFactsGenerated: Long = 0
        iter.foreach { row =>
            numFactsGenerated += 1
            set.ifNotExistsInsert(row, diffSet)
        }
        new HashSetPartition(diffSet, numFactsGenerated, diffSet.size(), numFields)
    }
}

object HashSetPartition {
    def apply(iter: Iterator[InternalRow], numFields: Int): HashSetPartition = {
        val set: InternalSet = if (numFields == 2) new TupleSet() else new IntSet()
        iter.foreach(next =>  set.insert(next))
        new HashSetPartition(set, numFields)
    }
}