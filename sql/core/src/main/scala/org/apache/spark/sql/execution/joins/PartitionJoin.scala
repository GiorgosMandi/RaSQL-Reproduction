package org.apache.spark.sql.execution.joins

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Projection, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.util.collection.CompactBuffer

trait PartitionJoin {
    self: SparkPlan =>

    val leftKeys: Seq[Expression]
    val rightKeys: Seq[Expression]
    val buildSide: BuildSide
    val left: SparkPlan
    val right: SparkPlan

    protected lazy val (buildPlan, streamedPlan) = buildSide match {
        case BuildLeft => (left, right)
        case BuildRight => (right, left)
    }

    protected lazy val (buildKeys, streamedKeys) = buildSide match {
        case BuildLeft => (leftKeys, rightKeys)
        case BuildRight => (rightKeys, leftKeys)
    }

    override def output: Seq[Attribute] = left.output ++ right.output

    override def outputsUnsafeRows: Boolean = true
    override def canProcessUnsafeRows: Boolean = true
    override def canProcessSafeRows: Boolean = false

    protected def buildSideKeyGenerator: Projection =
        UnsafeProjection.create(buildKeys, buildPlan.output)

    protected def streamSideKeyGenerator: Projection =
        UnsafeProjection.create(streamedKeys, streamedPlan.output)

    protected def partitionJoin(hashedRelationA: HashedRelation,
                              numStreamRows: LongSQLMetric,
                              hashedRelationB: HashedRelation,
                              numOutputRows: LongSQLMetric): Iterator[InternalRow] =
    {
        new Iterator[InternalRow] {
            private[this] var currentRowA: InternalRow = _
            private[this] var currentMatches: Seq[InternalRow] = _
            private[this] var currentMatchPosition: Int = -1
            val relationTableA: util.HashMap[UnsafeRow, CompactBuffer[UnsafeRow]] = hashedRelationA.asInstanceOf[UnsafeHashedRelation].hashTable
            val relationKeysA: util.Iterator[UnsafeRow] = relationTableA.keySet().iterator()

            // Mutable per row objects.
            private[this] val joinRow = new JoinedRow
            private[this] val resultProjection: (InternalRow) => InternalRow =
                UnsafeProjection.create(self.schema)

            private[this] val joinKeys = streamSideKeyGenerator

            override final def hasNext: Boolean =
                (currentMatchPosition != -1 && currentMatchPosition < currentMatches.size) ||
                    (relationKeysA.hasNext && fetchNext())

            override final def next(): InternalRow = {
                val ret = buildSide match {
                    case BuildRight => joinRow(currentRowA, currentMatches(currentMatchPosition))
                    case BuildLeft => joinRow(currentMatches(currentMatchPosition), currentRowA)
                }
                currentMatchPosition += 1
                numOutputRows += 1
                resultProjection(ret)
            }

            /**
             * Searches the streamed iterator for the next row that has at least one match in hashtable.
             *
             * @return true if the search is successful, and false if the streamed iterator runs out of
             *         tuples.
             */
            private final def fetchNext(): Boolean = {
                currentMatches = null
                currentMatchPosition = -1
                while (currentMatches == null && relationKeysA.hasNext) {
                    val rowIter =  hashedRelationA.get(relationKeysA.next()).iterator
                    while(rowIter.hasNext) {
                        currentRowA = rowIter.next()
                        numStreamRows += 1
                        val key = joinKeys(currentRowA)
                        if (!key.anyNull) {
                            currentMatches = hashedRelationB.get(key)
                        }
                    }
                }

                if (currentMatches == null) {
                    false
                } else {
                    currentMatchPosition = 0
                    true
                }
            }
        }
    }
}
