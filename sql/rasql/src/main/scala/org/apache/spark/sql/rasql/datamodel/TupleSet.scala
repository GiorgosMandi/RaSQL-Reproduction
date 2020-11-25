package org.apache.spark.sql.rasql.datamodel

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

class TupleSet extends InternalSet {
    type T = (Int, Int)
    var set: mutable.HashSet[(Int, Int)] = mutable.HashSet()

    override def insert(row: InternalRow): Unit = set.add(getKey(row), getValue(row))

    def ifNotExistsInsert(row: InternalRow, diffSet: InternalSet): Unit = {
        val longDiffSet = diffSet.asInstanceOf[TupleSet]
        val key = getKey(row)
        val value = getValue(row)
        if (! set.contains((key, value)))
            longDiffSet.set.add((key, value))
    }

    def union(other: InternalSet): Unit = set ++= other.asInstanceOf[TupleSet].set

    def getKey(row: InternalRow): Int = row.getInt(0)

    def getValue(row: InternalRow): Int = row.getInt(1)

    def iterator: Iterator[(Int, Int)] = set.iterator

    override def size(): Int = set.size


}