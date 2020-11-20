package org.apache.spark.sql.rasql.execution

import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.SetRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.fixedpoint.FixedPointJobDefinition
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.rasql.RaSQLContext

import scala.reflect.ClassTag


case class AggregateRecursion(name : String, left : SparkPlan, right : SparkPlan) extends BinaryNode {

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
        all.count()
        deltaS = all

        rasqlContext.setRecursiveRDD(rasqlContext.recursiveTable, rowRDD)
        val fpjd = new FixedPointJobDefinition(doRecursion, null)

        this.sparkContext.runFixedPointJob(deltaS, fpjd, partitionNotEmpty)
        all
    }


    def doRecursion(fpjd: FixedPointJobDefinition, allRDD: RDD[_]): RDD[InternalRow] = {

        // delta = all - new aggregation
        val ra = right.execute()
        deltaS = all.diff(ra).setName("delta"+iteration)

        // all = all U delta
        all = all.union(deltaS).setName("all"+iteration)
        fpjd.finalRDD = all
        all.cache()

        // deltaS = deltaS'
        iteration += 1

        val allCount = all.count()
        val deltaCount = deltaS.count()
        rasqlContext.setRecursiveRDD(rasqlContext.recursiveTable, all)
        // todo cleans up to 3 previous cached RDDs

        logInfo("Aggregate Recursion iteration: " + iteration)
        logInfo("All RDD size = " + allCount)
        logInfo("New Delta RDD size = " + deltaCount)

        if (deltaCount > 0)
            doRecursion(fpjd, all)
        deltaS
    }

}
