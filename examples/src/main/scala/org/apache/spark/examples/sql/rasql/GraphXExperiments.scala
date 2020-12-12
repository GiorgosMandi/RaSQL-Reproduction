package org.apache.spark.examples.sql.rasql

import java.util.Calendar

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object GraphXExperiments {


    /**
     *  Run by executing:
     *      bin/spark-submit --master < spark | yarn |local > --class org.apache.spark.examples.sql.rasql.GraphXExperiments  \
     *      examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.2.0.jar  < arguments >
     *  Arguments:
     *      -g /path/to/graph : path to file with the input graph
     *      -q query : predefined queries (CC, SSSP, REACH)
     *      -v X: define the starting vertex
     *      -p partitions: Define number of partitions
     *      -i X: set pregel max iterations
     *
     * @param args command line arguments
     */
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        GraphXUtils.registerKryoClasses(conf)
        val sc = new SparkContext(conf.setAppName("GraphX-Experiments"))

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
                case ("-p" | "-partitions") :: value :: tail =>
                    nextOption(map ++ Map("partitions" -> value), tail)
                case ("-v" | "-vertex") :: value :: tail =>
                    nextOption(map ++ Map("startVertex" -> value), tail)
                case ("-i" | "-iterations") :: value :: tail =>
                    nextOption(map ++ Map("iterations" -> value), tail)
                case _ :: tail =>
                    log.warn("RASQL: Unrecognized argument")
                    nextOption(map, tail)
            }
        }
        val argList = args.toList
        type OptionMap = Map[String, String]
        val options = nextOption(Map(), argList)

        val graphPath = options.getOrElse("graph", null)
        if(graphPath == null){
            log.error("No input Graph, use \"-g path_to_graph\" to specify your graph.")
            sc.stop()
            return
        }
        val partitions : Int  = options.getOrElse("partitions", "12").toInt
        val maxIterations: Int = options.getOrElse("iterations", "20").toInt

        val startTime = Calendar.getInstance().getTimeInMillis
        val graphRDD = GraphLoader.edgeListFile(sc, graphPath,
            numEdgePartitions = partitions).cache()


        val vertex = options.getOrElse("vertex", "1").toInt
        val results =  options.getOrElse("algorithm", "CC") match {
            case "CC" =>
                /**
                 * Connected Components Algorithm (CC)
                 * My implementation as the default acts like the graph is non-directed
                 */
                val initialGraph: Graph[Int, Int] = graphRDD.mapVertices((id, attr) => id.toInt)

                /**
                 * Dst gets the cmpId of src if its smaller
                 * otherwise dst cmpId does not change
                 * The algorithm exits when it reaches max iterations
                 */
                def sendMessage(edge: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
                    if (edge.srcAttr < edge.dstAttr)
                        Iterator((edge.dstId, edge.srcAttr))
                    else if (edge.srcAttr == edge.dstAttr)
                        Iterator.empty
                    else Iterator((edge.dstId, edge.dstAttr))
                }

                val cc = Pregel(initialGraph, Int.MaxValue, maxIterations = maxIterations)(
                    vprog = (id, attr, msg) => math.min(attr, msg),
                    sendMsg = sendMessage,
                    mergeMsg = (a, b) => math.min(a, b))

                cc.vertices.map { case (v1, v2) => v2 }.distinct().collect()

            case "SSSP" =>
                /**
                 * Single-Source Shortest Path (SSSP)
                 * Finds the shortest path from source to all nodes
                 * */
                val initialGraph: Graph[Int, Int] = graphRDD.mapVertices((id, _) => if (id == vertex) 0 else Int.MaxValue)

                /**
                 * if the examined path is smaller than the existing, update,
                 * else exit. The graph is initialized with max Int value for
                 * path to each vertex.
                 */
                def sendMessage(edge: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
                    if (edge.srcAttr + edge.attr < edge.dstAttr)
                        Iterator((edge.dstId, edge.srcAttr + edge.attr))
                    else
                        Iterator.empty
                }
                val sssp = initialGraph.ops.pregel(Int.MaxValue)(
                    vprog = (id, attr, msg) => math.min(attr, msg),
                    sendMsg = sendMessage,
                    mergeMsg = (a, b) => math.min(a, b))
                sssp.vertices.collect()

            case "REACH" =>
                /**
                 *  Reachability (REACH)
                 *  Find the vertices that are reachable from the initial node
                 */
                val initialGraph: Graph[Boolean,Int] = graphRDD.mapVertices((id, _) => if (id == vertex) true else false)

                /**
                 *  If the source is reachable, then make destination also reachable (1).
                 *  In the initial graph, all vertices are set to in-reachable (0) except the initial
                 *  vertex
                 */
                def sendMessage(edge: EdgeTriplet[Boolean, Int]): Iterator[(VertexId, Boolean)] = {
                    if (edge.srcAttr && edge.dstAttr)
                        Iterator.empty
                    else if (edge.srcAttr)
                        Iterator((edge.dstId, true))
                    else
                        Iterator((edge.dstId, false))
                }
                val reach = initialGraph.ops.pregel(initialMsg = false, maxIterations = maxIterations)(
                    vprog = (id, attr, msg) => (attr || msg),
                    sendMsg = sendMessage,
                    mergeMsg = (a, b) => (a || b))
                reach.vertices.collect()
        }

        log.info("\n" + results.mkString("\n"))
        log.info(results.length)
        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("Background Time: " + (endTime - startTime) / 1000.0 + "\n")
        sc.stop()
    }
}
