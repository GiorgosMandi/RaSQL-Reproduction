package org.apache.spark.sql.rasql.execution

import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.SetRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.rasql.RaSQLContext

import scala.reflect.ClassTag


case class RecursiveAggregate(name : String, left : SparkPlan, right : SparkPlan) extends BinaryNode {

    override def output: Seq[Attribute] = right.output

    var all: SetRDD = _
    var deltaS: SetRDD = _
    var iteration: Int = 0

    @transient
    final val rasqlContext: RaSQLContext = SQLContext.getActive().get.asInstanceOf[RaSQLContext]

    def partitionNotEmpty[U: ClassTag](iter: Iterator[_]): Boolean = iter.nonEmpty

    def doExecute(): RDD[InternalRow] = {
        val rowRDD: RDD[InternalRow] = left.execute()
        all = SetRDD(rowRDD, schema).setName("all"+iteration)
        deltaS = all
        val items = all.count()
        rasqlContext.setRecursiveRDD(rasqlContext.recursiveTable, rowRDD)
        doRecursion(items)
        all
    }


    def doRecursion(items: Long): Unit = {
        var newItems = items
        while(newItems > 0){
            iteration += 1
            // calculate the new items
            val ra = right.execute()

            // delta = all - new
            deltaS = all.diff(ra).setName("delta"+iteration)

            // all = all U delta
            all = all.union(deltaS).setName("all"+iteration)
            all.cache()

            newItems = deltaS.count()
            val allCount = all.count()
            rasqlContext.setRecursiveRDD(rasqlContext.recursiveTable, all)

            if (iteration >= 4 && newItems > 0)
                sparkContext.
                    getPersistentRDDs.values
                    .filter(rdd => rdd.name == "all"+(iteration-2) || rdd.name == "all"+(iteration-3))
                    .foreach(_.unpersist())

            logInfo("Aggregate Recursion iteration: " + iteration)
            logInfo("All RDD size = " + allCount)
            logInfo("New Delta RDD size = " + newItems)
        }
    }

}
