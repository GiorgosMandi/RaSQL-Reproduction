package org.apache.spark.examples.sql.rasql

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//  build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests -pl :spark-rasql_2.10 clean package
//  build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests package
//  bin/spark-submit --master local[*] --class org.apache.spark.examples.sql.rasql.RaSQLExperiment  examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.4.0.jar
//
//  bin/spark-submit --master local[*] --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --class org.apache.spark.examples.sql.rasql.RaSQLExperiment  examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.4.0.jar


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.rasql.RaSQLContext

object RaSQLExperiment {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("RaSQL-CC-Experiment")
        val sc = new SparkContext(sparkConf)
        val rasqlContext = new RaSQLContext(sc)

        val CCQuery = """ WITH recursive cc(Src, mmin AS CmpId) AS (SELECT Src, Src FROM edge) UNION (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src) SELECT count(distinct cc.CmpId) FROM cc"""
        val graph = Seq[(Long, Long)]( (1, 2), (2, 3), (3, 4), (4, 1), (5, 6), (6, 5)) //,(5, 3), (5, 6), (6, 5), (7, 5))
        val edgesRDD: RDD[(Long, Long)] = sc.parallelize[(Long, Long)](graph)
        val edgesDF = rasqlContext.createDataFrame(edgesRDD, "edge").toDF("Src", "Dst")
        edgesDF.cache()
        edgesDF.registerTempTable("edge")
        val cc = rasqlContext.sql(CCQuery).count()

        println("G: " + graph.head + "\n\nCC: " + cc + " \n\n\n")

        sc.stop()
    }
}

