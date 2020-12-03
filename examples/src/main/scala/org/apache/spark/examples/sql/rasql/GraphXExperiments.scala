package org.apache.spark.examples.sql.rasql

import java.util.Calendar

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx.{Graph, GraphLoader, GraphXUtils}
import org.apache.spark.{SparkConf, SparkContext}

object GraphXExperiments {
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
        val startTime = Calendar.getInstance().getTimeInMillis
        val graphRDD = GraphLoader.edgeListFile(sc, graphPath,
            numEdgePartitions = partitions).cache()


        val vertex = options.getOrElse("vertex", "1").toInt
        val results =  options.getOrElse("algorithm", "CC") match {
            case "CC" =>
                val cc = ConnectedComponents.run(graphRDD)
                cc.vertices.map { case (_, data) => data }.distinct().collect()
            case "SSSP" =>
                val weightedGraphRDD = graphRDD.mapEdges(e => e.attr.toDouble)

                val initialGraph = weightedGraphRDD.mapVertices((id, _) => if (id == vertex) 0.0 else Double.PositiveInfinity)
                val sssp = initialGraph.pregel(Double.PositiveInfinity)(
                        (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
                        triplet => {  // Send Message
                            if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                                Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
                            } else {
                                Iterator.empty
                            }
                        },
                        (a, b) => math.min(a, b) // Merge Message
                    )
                sssp.vertices.collect()
            case "CP" =>
                var g: Graph[(Int, Int), Int] = graphRDD.mapVertices((id, _) => (if (id == vertex) 0 else Int.MaxValue, 0)).cache()
                null
        }
        log.info(results.mkString("\n"))

        val endTime = Calendar.getInstance().getTimeInMillis
        log.info("Background Time: " + (endTime - startTime) / 1000.0 + "\n")
        sc.stop()
    }
}
