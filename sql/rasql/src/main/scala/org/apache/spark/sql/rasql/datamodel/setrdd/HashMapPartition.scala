package org.apache.spark.sql.rasql.datamodel.setrdd

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.rasql.PreMapFunction
import org.apache.spark.sql.rasql.datamodel.internalset.{InnerHashMap, InnerIterator}

import scala.reflect.ClassTag

/**
 * The structure of the partirion of the SetRDD
 * Works as a higher level wrapper for the InnerHashMap
 *
 * @param map the core InnerHashMap
 * @param numFactsGenerated numFactsGenerated
 * @param numFactsDerived numFactsDerived
 * @param pf The Pre-Map Function
 * @param cTag class tag
 */
class HashMapPartition(val map: InnerHashMap, numFactsGenerated: Long = 0, numFactsDerived: Long = 0, pf: PreMapFunction)(implicit val cTag: ClassTag[InternalRow])
    extends Serializable with org.apache.spark.Logging {

    def this(set: InnerHashMap, pf: PreMapFunction) = this(set, 0, 0, pf)

    def size: Long = map.size()

    def iterator: Iterator[InternalRow] = new InnerIterator(map)

    /**
     *  Applies union on partition level
     * @param otherPart other partition
     * @param rddId rdd id
     * @return the unioned partition
     */
    def union(otherPart: HashMapPartition, rddId: Int): HashMapPartition =
        otherPart match {
            case otherPart: HashMapPartition => {
                map.union(otherPart.map)
                new HashMapPartition(map, pf)
            }
            case _ => union(otherPart.iterator, rddId)
        }


    /**
     * Applies union on Iterator level
     * @param iter the iterator
     * @param rddId rdd id
     * @return the unioned partition
     */
    def union(iter: Iterator[InternalRow], rddId: Int): HashMapPartition = {
        val newMap = map
        iter.foreach(next => newMap.insert(next))
        new HashMapPartition(newMap, pf)
    }

    /**
     * Applies differentiation on Iterator level
     *
     * @param iter iterator
     * @param rddId rdd id
     * @return  the results of the iteration
     */
    def diff(iter: Iterator[InternalRow], rddId: Int): HashMapPartition = {
        val diffMap: InnerHashMap = new InnerHashMap(pf)
        var numFactsGenerated: Long = 0
        iter.foreach { row =>
            numFactsGenerated += 1
            map.ifNotExistsInsert(row, diffMap)
        }
        new HashMapPartition(diffMap, numFactsGenerated, diffMap.size(), pf)
    }
}

/**
 * Auxiliary constructor
 */
object HashMapPartition {
    def apply(iter: Iterator[InternalRow], pf: PreMapFunction): HashMapPartition = {
        val map: InnerHashMap = new InnerHashMap(pf)
        iter.foreach(next =>  map.insert(next))
        new HashMapPartition(map, pf)
    }
}