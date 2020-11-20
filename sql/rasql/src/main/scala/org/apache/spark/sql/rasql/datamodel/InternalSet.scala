package org.apache.spark.sql.rasql.datamodel

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

trait InternalSet {
    // TODO implement extension for string type
    type T
    var set: mutable.HashSet[T]

    def size(): Int

    def insert(row: InternalRow): Unit

    def ifNotExistsInsert(row: InternalRow, diffSet: InternalSet): Unit

    def union(other: InternalSet): Unit
}