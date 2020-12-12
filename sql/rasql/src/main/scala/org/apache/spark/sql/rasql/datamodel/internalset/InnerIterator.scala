package org.apache.spark.sql.rasql.datamodel.internalset

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *  Provides an iterator for the given InnerHashMap.
 *  Used mostly for writing the InnerHashMap into Internal Rows
 *
 * @param map InnerHashMap
 */
class InnerIterator(map: InnerHashMap) extends Iterator[InternalRow]{

    val rawIter: Iterator[(Int, Int)] = map.iterator
    val uRow = new UnsafeRow()
    val bufferHolder = new BufferHolder()
    val rowWriter = new UnsafeRowWriter()
    val numFields: Int =  2

    override final def hasNext(): Boolean = {
        rawIter.hasNext
    }

    // write the (K, V) into Row
    override final def next(): InternalRow = {
        bufferHolder.reset()
        rowWriter.initialize(bufferHolder, numFields)

        val value = rawIter.next()
        rowWriter.write(0, value._1)
        rowWriter.write(1, value._2)
        uRow.pointTo(bufferHolder.buffer, numFields, bufferHolder.totalSize())
        uRow
    }

    def get(): Seq[InternalRow] ={
        val rows: ListBuffer[InternalRow] = mutable.ListBuffer()
        while(hasNext()) rows.append(next())
        rows
    }
}