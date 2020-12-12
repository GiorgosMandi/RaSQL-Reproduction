package org.apache.spark.examples.sql.rasql


import java.util.Calendar

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions

object RaSQLExperiments {

    /**
     * Reading file into graph
     * UNFORTUNATELY, this is an old version of spark and it does not support the CSV Readers of databrick
     * @param filePath path of file
     * @param numPartitions target num of partitions
     * @param raSQLContext SQL context
     * @param addCost if true adds a column of aces with name "Cost" (case of SSSP)
     * @return the graph loaded as Dataframe with (Src, Ds, Cost) cols
     */
    def getGraphDF(filePath: String, numPartitions: Int, raSQLContext: RaSQLContext, addCost: Boolean = false): DataFrame = {
        val edgesRDD = raSQLContext.sparkContext.textFile(filePath)
            .filter(line => !line.trim.isEmpty && (line(0) != '%'))
            .map(line => {
                val splitLine = line.split("\t")
                (splitLine(0).toInt, splitLine(1).toInt)
            })
        var edgesDF = raSQLContext.createDataFrame(edgesRDD).toDF("Src", "Dst")
            .repartition(numPartitions)

        if (addCost) edgesDF = edgesDF.withColumn("Cost", functions.lit(1))
        edgesDF.repartition(12)
    }

    /**
     *  Run by executing:
     *      bin/spark-submit --master < spark | yarn |local > --class org.apache.spark.examples.sql.rasql.RaSQLExperiments  \
     *      examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.2.0.jar  < arguments >
     *  Arguments:
     *      -g /path/to/graph : path to file with the input graph
     *      -q query : predefined queries (CC, SSSP, REACH) or provide new query
     *      -v X: define the starting vertex
     *      -p partitions: Define number of partitions
     *
     * @param args command line arguments
     */
    def main(args: Array[String]) {

        // set loggers
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
                case ("-q" | "-query") :: value :: tail =>
                    nextOption(map ++ Map("query" -> value), tail)
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
            options.get("query") match {

                case Some("SSSP") =>
                    /***
                     * Single-Source Ghortest Path (SSSP):
                     *      Given a source vertex, find the shortest paths to all reachable vertices
                     *
                     *  Query:
                     *      WITH recursive path(Dst, mmin AS Cost) AS
                     *      | (SELECT 1, 0)
                     *      | UNION
                     *      | (SELECT edge.Dst, path.Cost + edge.Cost FROM path, edge WHERE path.Dst = edge.Src)
                     *      | SELECT Dst, Cost FROM path
                     */
                    isSSSP = true
                    raw"""WITH recursive path(Dst, mmin AS Cost) AS (SELECT $vertex, 0) UNION (SELECT edge.Dst, path.Cost + edge.Cost FROM path, edge WHERE path.Dst = edge.Src) SELECT Dst, Cost FROM path"""

                case Some("REACH") =>
                   /** REACH
                    *      Find all reachable vertices from given source. Typically implements a DFS
                    *
                    *  Query:
                    *       WITH recursive reach(Dst) AS
                    *       | (SELECT $vertex)
                    *       | UNION
                    *       | (SELECT edge.Dst FROM reach, edge WHERE reach.Dst = edge.Src)
                    *       | SELECT Dst FROM reach
                    **/
                    raw"""WITH recursive reach(Dst) AS (SELECT $vertex) UNION ( SELECT edge.Dst FROM reach, edge WHERE reach.Dst = edge.Src) SELECT Dst FROM reach"""

                case Some("CC") =>
                    /**
                     * Connected Components
                     *      Connected Components compromises all the induced sub-graph in which any two vertices are
                     *      connected to each other by paths. In our case, we consider the graphs to be directed.
                     *
                     * Query:
                     *      WITH recursive cc(Src, mmin AS CmpId) AS
                     *      | (SELECT Src, Src FROM edge)
                     *      | UNION
                     *      | (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src)
                     *      | SELECT count(distinct cc.CmpId) FROM cc
                     */
                    raw""" WITH recursive cc(Src, mmin AS CmpId) AS (SELECT Src, Src FROM edge) UNION (SELECT edge.Dst, cc.CmpId FROM cc, edge WHERE cc.Src = edge.Src) SELECT cc.Src, cc.CmpId FROM cc"""
                case Some(q) => q
                case _ =>
                    log.error("No query specified.")
                    return
            }

        val graphPath = options.getOrElse("graph", null)
        if(graphPath == null){
            log.error("No input Graph, use \"-g path_to_graph\" to specify your graph.")
            return
        }

        // Initialize and configure Spark and RaSQL context
        val partitions : Int  = options.getOrElse("partitions", "12").toInt
        val sparkConf = new SparkConf().setAppName("RaSQL-Experiment")
            .set("spark.default.parallelism", partitions.toString)
            .set("spark.shuffle.sort.bypassMergeThreshold", partitions.toString)
            .set("spark.sql.shuffle.partitions", partitions.toString)

        val sc = new SparkContext(sparkConf)
        val rasqlContext = new RaSQLContext(sc)

        val startTime = Calendar.getInstance().getTimeInMillis

        // Read graph into dataframe
        val edgesDF = getGraphDF(graphPath, partitions, rasqlContext, isSSSP)
        edgesDF.registerTempTable("edge")
        edgesDF.cache()

        // run specified query
        val results = rasqlContext.sql(query).collect()
        log.info("Printing results: \n")
        println(results.mkString("\n"))
        log.info("Total: " + results.length + "\n")

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("Background Time: " + (endTime - startTime) / 1000.0 + "\n")

        System.in.read()
        sc.stop()
    }
}

