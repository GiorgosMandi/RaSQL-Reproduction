package org.apache.spark.sql.rasql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

case class RelationCatalog(catalog: mutable.HashMap[String, RelationInfo] = mutable.HashMap.empty[String, RelationInfo]) extends Serializable {

    def addRelation(name : String, schema : StructType, rdd: RDD[InternalRow] = null) : Unit = {
        val relationInfo = RelationInfo(rdd, schema)
        catalog.get(name) match {
            case Some(old) => old.rdd = relationInfo.rdd
            case None => catalog.put(name, relationInfo)
        }
    }

    def setRDD(name : String, rdd : RDD[InternalRow]) : Unit = {
        catalog.get(name) match {
            case Some(old) => old.rdd = rdd
            case None => catalog.put(name, RelationInfo(rdd))
        }
    }

    def getRelationInfo(name : String) : RelationInfo = catalog.getOrElse(name, null)

    def getRDD(name : String) : RDD[InternalRow] = {
        val ri = getRelationInfo(name)
        if (ri != null) ri.rdd
        else null
    }

    def removeRDD(name : String) : Unit =  catalog.remove(name)

    def clear() : Unit = catalog.clear()

    override def toString: String = {
        val output = new StringBuilder()
        catalog.iterator.foreach(f => output.append(f.toString()))
        output.toString()
    }
}



case class RelationInfo(var rdd: RDD[InternalRow], var schema: StructType = null) extends Serializable {

    override def toString: String = "schema: " + this.schema + (if (rdd != null) " RDD")

}

sealed trait PreMapFunction

case object MMax extends PreMapFunction
case object MMin extends PreMapFunction
case object MSum extends PreMapFunction
case object MCount extends PreMapFunction