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


import java.util.Calendar

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RaSQLExperiment {


    def getGraph(sc: SparkContext, filePath: String, numPartitions: Int): RDD[(Int, Int)] = {
        sc.textFile(filePath, numPartitions)
            .coalesce(numPartitions)
            .filter(line => !line.trim.isEmpty && (line(0) != '%'))
            .map(line => {
                val splitLine = line.split("\t")
                (splitLine(0).toInt, splitLine(1).toInt)
            })
    }


    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("RaSQL-Experiment")
            .set("spark.shuffle.sort.bypassMergeThreshold", "12")
        val sc = new SparkContext(sparkConf)
        val rasqlContext = new RaSQLContext(sc)

        Logger.getLogger("org").setLevel(Level.INFO)
        Logger.getLogger("akka").setLevel(Level.INFO)
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        // Parsing input arguments
        @scala.annotation.tailrec
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case ("-g" | "-graph") :: value :: tail =>
                    nextOption(map ++ Map("graph" -> value), tail)
                case ("-a" | "-algorithm") :: value :: tail =>
                    nextOption(map ++ Map("algorithm" -> value), tail)
                case ("-q" | "-query") :: tail =>
                    nextOption(map ++ Map("query" -> null), tail)
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case _ :: tail =>
                    log.warn("RASQL: Unrecognized argument")
                    nextOption(map, tail)
            }
        }

        val argList = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), argList)

        val query: String =
            if(options.contains("algorithm")){
                options.get("algorithm") match {
                    case Some("CC") =>
                        """ WITH recursive cc(Src, mmin AS CmpId) AS (SELECT Src, Src FROM edge) UNION (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src) SELECT count(distinct cc.CmpId) FROM cc"""
                    case _ => null
                }
            }
            else if (options.contains("query")) readLine()
            else null

        if (query == null){
            log.error("No query specified.")
            sc.stop()
            return
        }

        val graphPath = options.getOrElse("graph", null)
        if(graphPath == null){
            log.error("No input Graph, use \"-g path_to_graph\" to specify your graph.")
            sc.stop()
            return
        }
        val partitions : Int  = options.getOrElse("partitions", "12").toInt

        val startTime = Calendar.getInstance().getTimeInMillis

         val edgesRDD = getGraph(sc, graphPath, partitions)
         val edgesDF = rasqlContext.createDataFrame(edgesRDD, "edge").toDF("Src", "Dst")
        edgesDF.registerTempTable("edge")
        edgesDF.cache()
        val cc = rasqlContext.sql(query).first()

        val endTime = Calendar.getInstance().getTimeInMillis

        log.info("Results: " + cc + " \n\n\n")
        log.info("Background Time: " + (endTime - startTime) / 1000.0)
        sc.stop()
    }
}

