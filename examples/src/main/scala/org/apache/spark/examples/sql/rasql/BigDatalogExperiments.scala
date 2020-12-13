package org.apache.spark.examples.sql.rasql

import java.util.Calendar

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.examples.datalog.Experiments.runBigDatalogReach
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.rasql.RaSQLContext
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.{SparkConf, SparkContext}
object BigDatalogExperiments {


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
     * Executed a BigDataog program
     *
     * @param bigDatalogCtx context
     * @param database      db
     * @param rules         datalog rules
     * @param query         datalog query
     * @param relations     input relation
     * @return              an RDD of the results
     */
    def runBigDatalogProgram(bigDatalogCtx: BigDatalogContext, database: String, rules: String,
                             query: String, relations: Seq[(String, String)]): RDD[Row] = {
        val objectText = database + "\n" + rules
        println(objectText)

        var result: RDD[Row] = null
        if (bigDatalogCtx.loadProgram(objectText)) {
            relations.foreach(relation => bigDatalogCtx.registerAndLoadTable(relation._1, relation._2, bigDatalogCtx.conf.numShufflePartitions))
            val program = bigDatalogCtx.query(query)
            result = program.execute()
        }

        bigDatalogCtx.reset()
        result
    }


    /**
     *  Run by executing:
     *      bin/spark-submit --master < spark | yarn |local > --class org.apache.spark.examples.sql.rasql.BigDatalogExperiments  \
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
                case ("-v" | "-startvertex") :: value :: tail =>
                    nextOption(map ++ Map("vertex" -> value), tail)
                case _ :: tail =>
                    log.warn("RASQL: Unrecognized argument")
                    nextOption(map, tail)
            }
        }
        val argList = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), argList)

        val filePath = options.getOrElse("graph", null)
        if(filePath == null){
            log.error("No input Graph, use \"-g path_to_graph\" to specify your graph.")
            return
        }

        // Initialize and configure Spark and context
        val partitions : Int  = options.getOrElse("partitions", "12").toInt
        val sparkConf = new SparkConf().setAppName("RaSQL-Experiment")
            .set("spark.default.parallelism", partitions.toString)
            .set("spark.shuffle.sort.bypassMergeThreshold", partitions.toString)
            .set("spark.sql.shuffle.partitions", partitions.toString)

        val sc = new SparkContext(sparkConf)
        val bigDatalogCtx = new BigDatalogContext(sc)
        val start = Calendar.getInstance().getTimeInMillis

        /**
         * Set datalog rules and query based on the input
         */
        val startVertexId = options.getOrElse("vertex", "1")
        val result: RDD[Row] =
            options.get("query") match {

                case Some("SSSP") =>
                    val database = "database({arc(From: integer, To: integer, Cost: integer)})."

                    val rules = "leftLinearSP(B,min<C>) <- mminleftLinearSP(B,C)." +
                        "mminleftLinearSP(B,mmin<C>) <- B=" + startVertexId + ", C=0." +
                        "mminleftLinearSP(B,mmin<D>) <- mminleftLinearSP(C,D1), arc(C,B,D2), D=D1+D2."

                    runBigDatalogProgram(bigDatalogCtx, database, rules, "leftLinearSP(A,B).", Seq(("arc", filePath)))

                case Some("REACH") =>
                    val startvertex = options("startvertex").toInt
                    runBigDatalogReach(bigDatalogCtx, filePath, options, startvertex)

                case Some("CC") =>
                    val database = "database({arc(From: integer, To: integer)})."

                    val rules = "cc3(X,mmin<X>) <- arc(X,_)." +
                        "cc3(Y,mmin<V>) <- cc3(X,V), arc(X,Y)." +
                        "cc2(X,min<Y>) <- cc3(X,Y)." +
                        "cc(countd<X>) <- cc2(_,X)."

                    runBigDatalogProgram(bigDatalogCtx, database, rules, "cc(A).", Seq(("arc", filePath)))
                case _ =>
                    log.error("No query specified.")
                    return
            }

        println("execution time: " + (System.currentTimeMillis() - start) + " ms, triangle closing size : " + result.count())

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("Background Time: " + (endTime - start) / 1000.0 + "\n")

        sc.stop()
    }
}


