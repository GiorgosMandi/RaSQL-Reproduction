package org.apache.spark.sql.rasql.datamodel

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

class LongSet extends InternalSet {
    type T = Long
    var set: mutable.HashSet[Long] = mutable.HashSet()

    override def insert(row: InternalRow): Unit = set.add(getKey(row))

    def ifNotExistsInsert(row: InternalRow, diffSet: InternalSet): Unit = {
        val longDiffSet = diffSet.asInstanceOf[LongSet]
        val key = getKey(row)
        if (!set.contains(key))
            longDiffSet.set.add(key)
    }

    def union(other: InternalSet): Unit = set ++= other.asInstanceOf[LongSet].set


    def getKey(row: InternalRow): Long = row.getLong(0)

    def iterator: Iterator[Long] = set.iterator

    override def size(): Int = set.size


}