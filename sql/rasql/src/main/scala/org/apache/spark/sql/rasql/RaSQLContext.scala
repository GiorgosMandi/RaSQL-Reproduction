package org.apache.spark.sql.rasql;

import org.apache.spark.sql.execution.{CacheManager, SparkSQLParser}
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.SQLContext


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

}

