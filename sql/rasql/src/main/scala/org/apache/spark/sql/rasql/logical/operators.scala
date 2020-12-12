package org.apache.spark.sql.rasql.logical

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._

case class RecursiveRelation(tableIdentifier: TableIdentifier, var output: Seq[Attribute]) extends LeafNode {

    def tableName: String = tableIdentifier.unquotedString

    override def statistics: Statistics = Statistics(Long.MaxValue)
}


case class RecursiveAggregate(name: String, left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
    // left is exitRules plan
    // right is recursive rules plan
    override def output: Seq[Attribute] = right.output
}

/**
 * A hint for the optimizer that we should cache the `child` if used in a join operator.
 */
case class CacheHint(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
}