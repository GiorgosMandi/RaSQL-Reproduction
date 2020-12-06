package org.apache.spark.sql.rasql.datamodel.internalset

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

class IntSet extends InternalSet {
    type T = Int

    var set: mutable.HashSet[Int] = mutable.HashSet()

    val numFields = 1

    override def insert(row: InternalRow): Unit = set.add(getKey(row))

    def ifNotExistsInsert(row: InternalRow, diffSet: InternalSet): Unit = {
        val longDiffSet = diffSet.asInstanceOf[IntSet]
        val key = getKey(row)
        if (!set.contains(key))
            longDiffSet.set.add(key)
    }

    def union(other: InternalSet): Unit = set ++= other.asInstanceOf[IntSet].set

    def getKey(row: InternalRow): Int = row.getInt(0)

    def iterator: Iterator[Int] = set.iterator

    override def size(): Int = set.size

}
