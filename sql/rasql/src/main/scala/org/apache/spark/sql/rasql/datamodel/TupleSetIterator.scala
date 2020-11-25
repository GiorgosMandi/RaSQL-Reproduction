package org.apache.spark.sql.rasql.datamodel

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}


class TupleSetIterator(set: TupleSet) extends Iterator[InternalRow]{

    val rawIter: Iterator[(Int, Int)] = set.iterator
    val uRow = new UnsafeRow()
    val bufferHolder = new BufferHolder()
    val rowWriter = new UnsafeRowWriter()
    val numFields = 2

    override final def hasNext(): Boolean = {
        rawIter.hasNext
    }

    override final def next(): InternalRow = {
        bufferHolder.reset()
        rowWriter.initialize(bufferHolder, numFields)

        val value = rawIter.next()
        rowWriter.write(0, value._1)
        rowWriter.write(1, value._2)
        uRow.pointTo(bufferHolder.buffer, numFields, bufferHolder.totalSize())
        uRow
    }

}