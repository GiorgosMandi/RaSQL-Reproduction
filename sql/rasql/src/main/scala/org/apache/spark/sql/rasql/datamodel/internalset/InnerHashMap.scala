package org.apache.spark.sql.rasql.datamodel.internalset

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable
import org.apache.spark.sql.rasql.{MCount, MMax, MMin, MSum, PreMapFunction}


class InnerHashMap(prem: PreMapFunction) {

    var map: mutable.HashMap[Int, Int] = mutable.HashMap()

    val numFields = 2

    def applyPreM(newValue: Int, value: Int): Int ={
        prem match {
            case MMin => math.min(newValue, value)
            case MMax => math.max(newValue, value)
            case MSum | MCount => newValue + value
        }
    }

    def shouldInsert(newValue: Int, value: Int): Boolean ={
        prem match {
            case MMin => newValue < value
            case MMax => newValue > value
            case MSum | MCount => true
        }
    }

    def insert(row: InternalRow): Unit = {
        val key = getKey(row)
        val value = getValue(row)
        if (map.contains(key)) map += key -> applyPreM(value, map(key))
        else map += key -> value
    }

    def ifNotExistsInsert(row: InternalRow, diffSet: InnerHashMap): Unit = {
        val key = getKey(row)
        val value = getValue(row)
        if (! map.contains(key))
            diffSet.insert(row)
        else if (shouldInsert(value, map(key)))
            diffSet.insert(row)
    }

    def union(other: InnerHashMap): Unit = map ++= other.map

    def getKey(row: InternalRow): Int = row.getInt(0)

    def getValue(row: InternalRow): Int = if (row.numFields == 2)  row.getInt(1) else 0

    def iterator: Iterator[(Int, Int)] = map.iterator

    def size(): Int = map.size

}