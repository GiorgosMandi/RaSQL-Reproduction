package org.apache.spark.sql.rasql.datamodel.internalset

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

trait InternalSet {
    type T
    var set: mutable.HashSet[T]

    val numFields: Int

    def size(): Int

    def insert(row: InternalRow): Unit

    def ifNotExistsInsert(row: InternalRow, diffSet: InternalSet): Unit

    def union(other: InternalSet): Unit

    def iterator: Iterator[T]

}