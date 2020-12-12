package org.apache.spark.sql.rasql;

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.execution.{CacheManager, SparkSQLParser}
import org.apache.spark.sql.rasql.logical._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{HashPartitioner, Logging, SparkContext}


class RaSQLContext(@transient override val sparkContext: SparkContext,
                   @transient override val cacheManager: CacheManager,
                   @transient override val listener: SQLListener,
                   override val isRootContext: Boolean)
    extends SQLContext(sparkContext, cacheManager, listener, isRootContext)
        with Serializable
        with Logging {

    self =>

    def this(sparkContext: SparkContext) = { this(sparkContext, new CacheManager, SQLContext.createListenerAndUI(sparkContext), true)}

    @transient
    protected[sql] override val sqlParser = new SparkSQLParser(RaSQLParser.parse)

    @transient
    override lazy val analyzer: Analyzer = new Analyzer(catalog, functionRegistry, conf) {}

    override val planner: RaSQLPlanner = new RaSQLPlanner(this)

    val relationCatalog: RelationCatalog = RelationCatalog()

    var recursiveTable: String = _

    var partitions: Int = this.sparkContext.getConf.get("spark.default.parallelism").toInt

    val hashPartitioner: HashPartitioner = new HashPartitioner(partitions)


    def internalCreateDataFrame(name: String, rdd: RDD[InternalRow], schema: StructType): DataFrame = {
        relationCatalog.addRelation(name, schema, rdd)
        internalCreateDataFrame(rdd, schema)
    }

    def setRecursiveRDD(name: String, rdd: RDD[InternalRow]): Unit = {
        relationCatalog.setRDD(name, rdd)
    }

    def getRDD(name: String): RDD[InternalRow] = {
        val relationInfo = relationCatalog.getRelationInfo(name)
        if (relationInfo != null)
            relationInfo.rdd
        else null
    }

    var preMapF: PreMapFunction = MMin
    def setPremF(f: AggregateFunction): Unit =
        preMapF = f match {
            case logical.MMax(_) => MMax
            case logical.MMin(_) => MMin
            case logical.MSum(_) => MSum
            case logical.MCount(_) => MCount
        }

    val mmin: (String, (ExpressionInfo, FunctionBuilder)) = FunctionRegistry.expression[MMin](name="mmin")
    functionRegistry.registerFunction("mmin", info = mmin._2._1, builder = mmin._2._2)

    val mmax: (String, (ExpressionInfo, FunctionBuilder)) = FunctionRegistry.expression[MMax](name="mmax")
    functionRegistry.registerFunction("mmax", info = mmax._2._1, builder = mmax._2._2)

    val msum: (String, (ExpressionInfo, FunctionBuilder)) = FunctionRegistry.expression[MSum](name="msum")
    functionRegistry.registerFunction("msum", info = msum._2._1, builder = msum._2._2)

    val mcount: (String, (ExpressionInfo, FunctionBuilder)) = FunctionRegistry.expression[MCount](name="mcount")
    functionRegistry.registerFunction("mcount", info = mcount._2._1, builder = mcount._2._2)

}

