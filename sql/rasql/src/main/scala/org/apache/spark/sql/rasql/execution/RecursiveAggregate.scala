package org.apache.spark.sql.rasql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.rasql.{PreMapFunction, RaSQLContext}
import org.apache.spark.sql.rasql.datamodel.setrdd.SetRDD

import scala.reflect.ClassTag

/**
 * Recursive Aggregate, the core of the whole implementation.
 *
 * Simply, first executes the left query which is the base query, and stores it as the recursive RDD.
 * Then applies the right query, which is the recursive one, that joins the base relation with the recursive RDD
 * Both left, and right, contain partial aggregation
 *
 * Then we find the new records by differentiate the old form the new, and adding the to the old
 * We repeat until we have no new records
 *
 * Both new (delta) and old (all) are stored as SetRDD which applies the Pre-Map functions within.
 *
 * @param name  name
 * @param left the base query
 * @param right the recursive query
 */
case class RecursiveAggregate(name : String, left : SparkPlan, right : SparkPlan) extends BinaryNode {

    override def output: Seq[Attribute] = right.output

    var all: SetRDD = _
    var delta: SetRDD = _
    var iteration: Int = 0

    @transient
    final val rasqlContext: RaSQLContext = SQLContext.getActive().get.asInstanceOf[RaSQLContext]

    final lazy val premFunction: PreMapFunction= rasqlContext.preMapF

    def partitionNotEmpty[U: ClassTag](iter: Iterator[_]): Boolean = iter.nonEmpty

    /**
     * - Applies the base query
     * - Sets the recursive RDD
     * - Start the recursion
     *
     * @return the results
     */
    def doExecute(): RDD[InternalRow] = {

        val rowRDD: RDD[InternalRow] = left.execute()

        all = SetRDD(rowRDD, premFunction).setName("all"+iteration)
        all.cache()
        delta = all
        val items = all.collect()

        rasqlContext.setRecursiveRDD(rasqlContext.recursiveTable, rowRDD)
        doRecursion(items.length)
        all
    }


    /**
     *  Iteratively do
     *      - Calculate the new records, using the recursive RDD
     *      - Differentiate to find the new records
     *      - Union them to the old ones
     *      - Update the recursive RDD
     * Iteration stops when no new records were discovered
     * All RDDs are stored as SetRDDs
     *
     * @param items new items before the iteration
     */
    def doRecursion(items: Long): Unit = {
        var newItems = items
        while(newItems > 0){
            iteration += 1

            // calculate the new items
            val delta_ = SetRDD(right.execute(), premFunction).setName("delta_"+iteration)
            delta_.cache()

            // delta = all - new
            delta = all.diff(delta_).setName("delta"+iteration)
            delta.cache()
            newItems = delta.count()

            // all = all U delta
            all = all.union(delta).setName("all"+iteration)
            all.cache()

            // delta becomes the new Recursive RDD for the next iteration
            rasqlContext.setRecursiveRDD(rasqlContext.recursiveTable, delta_)

            if (iteration >= 4 && newItems > 0)
                sparkContext.
                    getPersistentRDDs.values
                    .filter(rdd => rdd.name == "delta"+(iteration-2) || rdd.name == "delta"+(iteration-3) ||
                        rdd.name == "all"+(iteration-2) || rdd.name == "all"+(iteration-3) ||
                        rdd.name == "delta_"+(iteration-2) || rdd.name == "delta_"+(iteration-3))
                    .foreach(_.unpersist())

            logInfo("Aggregate Recursion iteration: " + iteration)
            logInfo("New Delta RDD size = " + newItems)
        }
    }

}
