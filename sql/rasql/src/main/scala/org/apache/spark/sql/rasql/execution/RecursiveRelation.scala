package org.apache.spark.sql.rasql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.rasql.RaSQLContext

case class RecursiveRelation(name: String, output: Seq[Attribute]) extends LeafNode {

    @transient
    final val rasqlContext =this.sqlContext.asInstanceOf[RaSQLContext]

    override def simpleString: String = s"$nodeName " + output.mkString("[", ",", "]") + "(" + name + ")"

    override def doExecute(): RDD[InternalRow] = rasqlContext.getRDD(name)
}
