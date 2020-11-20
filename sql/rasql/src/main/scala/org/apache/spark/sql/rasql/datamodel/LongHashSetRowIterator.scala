package org.apache.spark.sql.rasql.datamodel

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}


class LongHashSetRowIterator(set: LongSet) extends Iterator[InternalRow]{

    val rawIter: Iterator[Long] = set.iterator
    val numFields = 2 // Todo find the number of fields
    val uRow = new UnsafeRow()
    val bufferHolder = new BufferHolder()
    val rowWriter = new UnsafeRowWriter()

    override final def hasNext(): Boolean = {
        rawIter.hasNext
    }

    override final def next(): InternalRow = {
        bufferHolder.reset()
        rowWriter.initialize(bufferHolder, numFields)

        val value = rawIter.next()
        rowWriter.write(0, value)
        uRow.pointTo(bufferHolder.buffer, numFields, bufferHolder.totalSize())
        uRow
    }

}