package org.apache.spark.sql.rasql.datamodel.setrdd

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rasql.datamodel.{TupleSetIterator, TupleSet}

import scala.reflect.ClassTag

class SetRDDHashSetPartition(val set: TupleSet, numFactsGenerated: Long = 0, numFactsDerived: Long = 0)(implicit val cTag: ClassTag[InternalRow])
    extends SetRDDPartition[InternalRow](numFactsGenerated, numFactsDerived) with Serializable {

    def this(set: TupleSet) = this(set, 0, 0)

    override def size: Long = set.size()

    override def iterator: Iterator[InternalRow] = new TupleSetIterator(set)

    override def union(otherPart: SetRDDPartition[InternalRow], rddId: Int): SetRDDHashSetPartition = {
        val start = System.currentTimeMillis()
        val newPartition = otherPart match {
            case otherPart: SetRDDHashSetPartition => {
                set.union(otherPart.set)
                new SetRDDHashSetPartition(set)
            }
            case _ => union(otherPart.iterator, rddId)
        }

        logInfo("Union set size %s for rdd %s took %s ms".format(set.size(), rddId, System.currentTimeMillis() - start))
        newPartition
    }

    override def union(iter: Iterator[InternalRow], rddId: Int): SetRDDHashSetPartition = {
        val start = System.currentTimeMillis()
        val newSet = set
        iter.foreach(next => newSet.insert(next))

        logInfo("Union set size %s for rdd %s took %s ms".format(set.size(), rddId, System.currentTimeMillis() - start))
        new SetRDDHashSetPartition(newSet)
    }

    override def diff(iter: Iterator[InternalRow], rddId: Int): SetRDDHashSetPartition = {
        val start = System.currentTimeMillis()
        val diffSet = new TupleSet()
        var numFactsGenerated: Long = 0
        iter.foreach { row =>
            numFactsGenerated += 1
            set.ifNotExistsInsert(row, diffSet)
        }
        logInfo("Diff set size %s for rdd %s took %s ms".format(diffSet.size(), rddId, System.currentTimeMillis() - start))
        new SetRDDHashSetPartition(diffSet, numFactsGenerated, diffSet.size())
    }
}

object SetRDDHashSetPartition {
    def apply(iter: Iterator[InternalRow]): SetRDDHashSetPartition = {
        val set = new TupleSet()
        iter.foreach(next =>  set.insert(next))
        new SetRDDHashSetPartition(set)
    }
}