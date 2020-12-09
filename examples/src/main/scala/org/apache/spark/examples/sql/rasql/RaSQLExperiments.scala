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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RaSQLExperiments {

    /**
     * Reading file into grapsh
     * UNFORTUNATELY, this is an old version of spark and it does not support the CSV Readers of databrick
     * @param filePath path of file
     * @param numPartitions target num of partitions
     * @param raSQLContext SQL context
     * @return the graph loaded as Dataframe with (Src, Ds, Cost) cols
     */
    def getGraphDF2(filePath: String, numPartitions: Int, raSQLContext: RaSQLContext): DataFrame = {
        val edgesRDD = raSQLContext.sparkContext.textFile(filePath)
            .filter(line => !line.trim.isEmpty && (line(0) != '%'))
            .map(line => {
                val splitLine = line.split("\t")
                (splitLine(0).toInt, splitLine(1).toInt)
            })
        raSQLContext.createDataFrame(edgesRDD, "edge").toDF("Src", "Dst")
    }

    /**
     * Reading file into grapsh
     * UNFORTUNATELY, this is an old version of spark and it does not support the CSV Readers of databrick
     * @param filePath path of file
     * @param numPartitions target num of partitions
     * @param raSQLContext SQL context
     * @return the graph loaded as Dataframe with (Src, Dst, Cost) cols
     */
    def getGraphDF3(filePath: String, numPartitions: Int, raSQLContext: RaSQLContext): DataFrame = {
        val edgesRDD = raSQLContext.sparkContext.textFile(filePath, numPartitions)
            .repartition(numPartitions)
            .filter(line => !line.trim.isEmpty && (line(0) != '%'))
            .map(line => {
                val splitLine = line.split("\t")
                (splitLine(0).toInt, splitLine(1).toInt, splitLine(2).toInt)
            })
        raSQLContext.createDataFrame(edgesRDD, "edge").toDF("Src", "Dst", "Cost")
    }


    def main(args: Array[String]) {

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
                case ("-v" | "-vertex") :: value :: tail =>
                    nextOption(map ++ Map("vertex" -> value), tail)
                case _ :: tail =>
                    log.warn("RASQL: Unrecognized argument")
                    nextOption(map, tail)
            }
        }
        val argList = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), argList)

        var isSSSP = false
        val vertex = options.getOrElse("vertex", "1")
        val query: String =
            if(options.contains("algorithm")){
                options.get("algorithm") match {
                    case Some("CC") =>
                        //  WITH recursive cc(Src, mmin AS CmpId) AS
                        //  | (SELECT Src, Src FROM edge)
                        //  | UNION
                        //  | (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src)
                        //  | SELECT count(distinct cc.CmpId) FROM cc
                        raw""" WITH recursive cc(Src, mmin AS CmpId) AS (SELECT Src, Src FROM edge) UNION (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src) SELECT count(distinct cc.CmpId) FROM cc"""
                    case Some("SSSP") =>
                        // WITH recursive path(Dst, mmin AS Cost) AS
                        // | (SELECT 5, 0)
                        // | UNION
                        // | (SELECT edge.Dst, path.Cost + edge.Cost FROM path, edge WHERE path.Dst = edge.Src)
                        //  SELECT Dst, Cost FROM path
                        isSSSP = true
                        raw"""WITH recursive path(Dst, mmin AS Cost) AS (SELECT $vertex, 0) UNION (SELECT edge.Dst, path.Cost + edge.Cost FROM path, edge WHERE path.Dst = edge.Src) SELECT Dst, Cost FROM path"""
                    case Some("CP") =>
                        // WITH recursive cpaths(Dst, msum AS Cnt) AS
                        // | (SELECT 1, 1)
                        // | UNION
                        // | ( SELECT edge.Dst, cpaths.Cnt FROM cpaths, edge WHERE cpaths.Dst = edge.Src)
                        // | SELECT Dst, Cnt FROM cpaths
                        raw"""WITH recursive cpaths(Dst, msum AS Cnt) AS (SELECT $vertex, 1) UNION ( SELECT edge.Dst, cpaths.Cnt FROM cpaths, edge WHERE cpaths.Dst = edge.Src) SELECT  Dst, Cnt FROM cpaths"""
                    case Some("REACH") =>
                        // WITH recursive reach(Dst) AS
                        // | (SELECT $vertex)
                        // | UNION
                        // | (SELECT edge.Dst FROM reach, edge WHERE reach.Dst = edge.Src)
                        // | SELECT Dst FROM reach
                        raw"""WITH recursive reach(Dst) AS (SELECT $vertex) UNION ( SELECT edge.Dst FROM reach, edge WHERE reach.Dst = edge.Src) SELECT Dst FROM reach"""
                    case _ => null
                }
            }
            else if (options.contains("query")) readLine()
            else null

        val partitions : Int  = options.getOrElse("partitions", "12").toInt

        val sparkConf = new SparkConf().setAppName("RaSQL-Experiment")
            .set("spark.default.parallelism", partitions.toString)
            .set("spark.shuffle.sort.bypassMergeThreshold", partitions.toString)
        val sc = new SparkContext(sparkConf)
        val rasqlContext = new RaSQLContext(sc)


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

        val startTime = Calendar.getInstance().getTimeInMillis

         val edgesDF = if (isSSSP) getGraphDF3(graphPath, partitions, rasqlContext)
                        else getGraphDF2(graphPath, partitions, rasqlContext)
        edgesDF.registerTempTable("edge")
        edgesDF.cache()
        val results = rasqlContext.sql(query).collect()
        log.info("Printing results: \n")
        println(results.mkString("\n"))
        log.info("Total: " + results.length + "\n")
        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("Background Time: " + (endTime - startTime) / 1000.0 + "\n")
        sc.stop()
    }
}

