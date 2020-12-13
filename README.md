# RsSQL on Spark 1.6.1
## Reproduction

----
*Jiaqi Gu et al. “RaSQL: Greater Power and Performance for Big Data Analytics with Recursive-aggregate-SQL on Spark”. 
In:Proceedings of the 2019 International Conference on Managementof Data, SIGMOD Conference 2019, Amsterdam,
 The Netherlands. [url](https://doi.org/10.1145/3299869.3324959)* 


This work is a reproduction of the above RaSQL publication, as a project I chose to do for the course Database Systems in the
 master program [DSIT](http://dsit.di.uoa.gr/), in the University of Athens. For more information you can address to my report
 of better to the actual publication

 
---
## RaSQL
**RaSQL**  is a system that extends **SparkSQL** with recursive aggregation enabling it to parse recursive SQL queries, 
especially useful for graph algorithms and data mining. The implementation is based on the the Algorithm,
described in the paper. Furthermore it exploits the advantages of PreM aggregation functions in order to reduce the size of
the intermediate data by applying the aggregation in an non-stratified way.

This is a forked repository of BigDatalog. BigDatalog is a very similar tool designed to execute Datalog queries over distributed datasets.

 
## Performance

In order to evaluate the performance of my implementation of RaSQL, I compared it with BigDatalog and GraphX which is component 
of Spark for parallel graph computations. For input datasets, I generated graphs using the [PaRMAT](https://github.com/farkhor/PaRMAT) graph generator, containing
millions of vertices and edges. As queries I examined three very popular graph algorithms

- Connected Components (CC): Finds all the connected components of a graph
- Single-Source Shortest Paths (SSSP): Finds the shortest paths to all reachable vertices given an input source 
- REACH: Finds the reachable nodes from a given source (performs a Depth First Search) 

The results are presented in the following Figure. As you can see RaSQL outperforms most of the time its competitors. 


 //  add figure 
 
 ## Build
Before building for the first time, run the following to install the front-end compiler into the maven's local repository. This is required in order to build BigDatalog:

    $ build/mvn install:install-file -Dfile=datalog/lib/DeALS-0.6.jar -DgroupId=DeALS -DartifactId=DeALS -Dversion=0.6 -Dpackaging=jar

Then follow the same procedures as Spark itself (see ["Building Spark"](http://spark.apache.org/docs/1.6.1/building-spark.html)). i.e. run
    
    $ build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests package
    

## Execution

You can execute a recursive query using RaSQL by running

    $ bin/spark-submit --master < spark-master >  --class org.apache.spark.examples.sql.rasql.RaSQLExperiments  examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.2.0.jar  -g /path/to/graph  -q < query> 
    
In more details the available flags are the following:

- -g path/to/graph: specify the path to the input graph
- -p P: to re-partition your data (optional)
- -v V: to specify the id of the initial vertex (in case of SSSP, REACH, etc) (optional, default=1)
- -q QUERY: to specify the input query, for instance  

        -q "WITH recursive cpaths (Dst, sum() AS Cnt) AS (SELECT 1, 1) \
            UNION (SELECT edge.Dst, cpaths.Cnt FROM cpaths, edge \
            WHERE cpaths.Dst = edge.Src) SELECT Dst, Cnt FROM cpaths"
            
    - Some pre-defined queries are 
        
        - `-q CC`: for counting the connected components 
        - `-q SSSP`: for Single-Source Shortest Paths
        - `-q REACH`: for REACH

Using the same flags you can execute the GraphX by running 
    
    $ bin/spark-submit --master < spark-master >  --class org.apache.spark.examples.sql.rasql.GraphXExperiments  examples/target/scala-2.10/spark-examples-1.6.1-hadoop2.2.0.jar  -g /path/to/graph  -q < query> 
