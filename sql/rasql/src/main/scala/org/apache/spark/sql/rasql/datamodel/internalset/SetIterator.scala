package org.apache.spark.sql.rasql.datamodel.internalset

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}


class SetIterator(set: InternalSet) extends Iterator[InternalRow]{

    val rawIter: Iterator[Any] = set.iterator
    val uRow = new UnsafeRow()
    val bufferHolder = new BufferHolder()
    val rowWriter = new UnsafeRowWriter()
    val numFields: Int =  set.numFields

    override final def hasNext(): Boolean = {
        rawIter.hasNext
    }

    override final def next(): InternalRow = {
        bufferHolder.reset()
        rowWriter.initialize(bufferHolder, numFields)

        val value = rawIter.next()
        value match {
            case value:(Int, Int) =>
                rowWriter.write(0, value._1)
                rowWriter.write(1, value._2)
                uRow.pointTo(bufferHolder.buffer, numFields, bufferHolder.totalSize())
            case value: Int =>
                rowWriter.write(0, value)
                uRow.pointTo(bufferHolder.buffer, numFields, bufferHolder.totalSize())
        }
        uRow
    }
}