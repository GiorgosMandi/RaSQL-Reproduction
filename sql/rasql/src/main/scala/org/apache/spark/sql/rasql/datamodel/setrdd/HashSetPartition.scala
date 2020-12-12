package org.apache.spark.sql.rasql.datamodel.setrdd

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rasql.PreMapFunction
import org.apache.spark.sql.rasql.datamodel.internalset.{InnerHashMap, InnerIterator}

import scala.reflect.ClassTag

class HashSetPartition(val set: InnerHashMap, numFactsGenerated: Long = 0, numFactsDerived: Long = 0, pf: PreMapFunction)(implicit val cTag: ClassTag[InternalRow])
    extends Serializable with org.apache.spark.Logging {

    def this(set: InnerHashMap, pf: PreMapFunction) = this(set, 0, 0, pf)

    def size: Long = set.size()

    def iterator: Iterator[InternalRow] = new InnerIterator(set)

    def union(otherPart: HashSetPartition, rddId: Int): HashSetPartition = {
        val newPartition = otherPart match {
            case otherPart: HashSetPartition => {
                set.union(otherPart.set)
                new HashSetPartition(set, pf)
            }
            case _ => union(otherPart.iterator, rddId)
        }
        newPartition
    }


    def union(iter: Iterator[InternalRow], rddId: Int): HashSetPartition = {
        val newSet = set
        iter.foreach(next => newSet.insert(next))
        new HashSetPartition(newSet, pf)
    }


    def diff(iter: Iterator[InternalRow], rddId: Int): HashSetPartition = {
        val diffSet: InnerHashMap = new InnerHashMap(pf)
        var numFactsGenerated: Long = 0
        iter.foreach { row =>
            numFactsGenerated += 1
            set.ifNotExistsInsert(row, diffSet)
        }
        new HashSetPartition(diffSet, numFactsGenerated, diffSet.size(), pf)
    }
}

object HashSetPartition {
    def apply(iter: Iterator[InternalRow], pf: PreMapFunction): HashSetPartition = {
        val set: InnerHashMap = new InnerHashMap(pf)
        iter.foreach(next =>  set.insert(next))
        new HashSetPartition(set, pf)
    }
}