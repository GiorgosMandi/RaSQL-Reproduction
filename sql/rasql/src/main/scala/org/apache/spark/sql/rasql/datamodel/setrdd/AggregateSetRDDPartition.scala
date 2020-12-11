
package org.apache.spark.sql.rasql.datamodel.setrdd

import java.util

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.KeyValueToInternalRowIterator
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMonotonicAggregationMap
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.rasql.execution.MonotonicAggregateGlobal

class AggregateSetRDDPartition(aggregateStore: UnsafeFixedWidthMonotonicAggregationMap, monotonicAggregate: MonotonicAggregateGlobal, numFields: Int) extends Serializable with Logging{

    def size: Int = aggregateStore.numElements()

    def iterator: Iterator[InternalRow] = KeyValueToInternalRowIterator(aggregateStore.iterator(), monotonicAggregate.generateResultProjection())

    /**
     * update() merges the results produced during the iteration into this partition
     *  during update():
     *   - the underlying aggregateSetRDDPartition storage is updated.
     *   - a 2nd aggregateSetRDDPartition is produced indicating the rows that changed during the merge
     *  This is similar to regular aggregation, just that we-use the same hashmap each iteration
     *
     * @param iter Iterator of this partitions
     * @param monotonicAggregate examined monotonic aggregation
     * @return
     */
    def update(iter: Iterator[InternalRow], monotonicAggregate: MonotonicAggregateGlobal): (AggregateSetRDDPartition, HashSetPartition) = {

        // this is going to perform the aggregation and return an iterator over the output
        val maIter = monotonicAggregate.getAggregationIterator(iter, aggregateStore)
        val hashMapIter = new JavaHashMapIterator(maIter.deltaSet, monotonicAggregate.generateResultProjection())

        (new AggregateSetRDDPartition(aggregateStore, monotonicAggregate, numFields), HashSetPartition(hashMapIter, numFields))
    }
}

class JavaHashMapIterator(hashMap: java.util.HashMap[UnsafeRow, UnsafeRow], resultProjection: (UnsafeRow, UnsafeRow) => UnsafeRow) extends Iterator[InternalRow] {
    val iterator: util.Iterator[util.Map.Entry[UnsafeRow, UnsafeRow]] = hashMap.entrySet().iterator()

    override def hasNext: Boolean = iterator.hasNext

    override def next: InternalRow = {
        val entry = iterator.next()
        resultProjection(entry.getKey, entry.getValue)
    }
}