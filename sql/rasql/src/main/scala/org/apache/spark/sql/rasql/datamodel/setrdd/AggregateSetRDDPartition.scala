
package org.apache.spark.sql.rasql.datamodel.setrdd

import java.util

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.KeyValueToInternalRowIterator
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMonotonicAggregationMap
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.rasql.execution.MonotonicAggregate

class AggregateSetRDDPartition(aggregateStore: UnsafeFixedWidthMonotonicAggregationMap, monotonicAggregate: MonotonicAggregate) extends Serializable with Logging{

    def size: Int = aggregateStore.numElements()

    def iterator: Iterator[InternalRow] = KeyValueToInternalRowIterator(aggregateStore.iterator(), monotonicAggregate.generateResultProjection())

    /**
     * TODO FIX
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
    def update(iter: Iterator[InternalRow], monotonicAggregate: MonotonicAggregate): (AggregateSetRDDPartition, SetRDDPartition[InternalRow]) = {

        val start = System.currentTimeMillis()
        val before = this.size
        // this is going to perform the aggregation and return an iterator over the output
        val maIter = monotonicAggregate.getAggregationIterator(iter, aggregateStore)

        logInfo("Update deltaSPrime set size before %s after %s, delta set size %s took %s ms"
            .format(before, aggregateStore.numElements(), maIter.deltaSet.size, System.currentTimeMillis() - start))

        val hashMapIter = new JavaHashMapIterator(maIter.deltaSet, monotonicAggregate.generateResultProjection())

        (new AggregateSetRDDPartition(aggregateStore, monotonicAggregate), SetRDDHashSetPartition(hashMapIter))
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