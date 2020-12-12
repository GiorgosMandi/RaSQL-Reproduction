package org.apache.spark.sql.rasql

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{Filter, SparkPlan, SparkPlanner, joins}
import org.apache.spark.sql.rasql.logical.CacheHint


/**
 * RaSQL Planner
 * Convert the Logical Plan into a Spark Plan (Physical Plan)
 * @param rc RaSQL context
 */
class RaSQLPlanner(val rc: RaSQLContext) extends SparkPlanner(rc) {
    self: RaSQLPlanner =>

    override def strategies: Seq[Strategy] = (RecursiveAggregation :: CachedEquiJoinSelection :: Nil) ++ super.strategies

    /**
     * Set Recursive Aggregate execution plan
     */
    object RecursiveAggregation extends Strategy {
        def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
            case logical.RecursiveAggregate(name, left, right) =>
                execution.RecursiveAggregate(name, planLater(left), planLater(right)) :: Nil
            case logical.RecursiveRelation(table, output) =>
                execution.RecursiveRelation(table.unquotedString, output) :: Nil
            case _ => Nil
        }
    }


    /**
     * Matches a plan whose partitions can be cached and re-used
     */
    object CanCache {
        def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
            case CacheHint(p) => Some(p)
            case _ => None
        }
    }

    /**
     * A Data shuffling strategy, in which we indicate to cache on of its children
     * The Equi Join is implemented as aShuffle Hash join, in order to avoid the Sort based join
     */
    object CachedEquiJoinSelection extends Strategy {

        private[this] def makeShuffleHashJoin(leftKeys: Seq[Expression],
                                              rightKeys: Seq[Expression],
                                              left: LogicalPlan,
                                              right: LogicalPlan,
                                              condition: Option[Expression],
                                              side: joins.BuildSide): Seq[SparkPlan] = {
            val shuffleHashJoin = execution.ShuffleHashJoin(leftKeys, rightKeys, side, planLater(left), planLater(right))
            condition.map(Filter(_, shuffleHashJoin)).getOrElse(shuffleHashJoin) :: Nil
        }


        def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
            case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, CanCache(right)) =>
                makeShuffleHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildRight)

            case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, CanCache(left), right) =>
                makeShuffleHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildLeft)

            case _ => EquiJoinSelection.apply(plan)
        }
    }


}
