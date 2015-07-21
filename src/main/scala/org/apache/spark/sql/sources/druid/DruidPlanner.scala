package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.sparklinedata.druid.DruidQueryBuilder


class DruidPlanner private[druid](val sqlContext : SQLContext) extends DruidTransforms {

  sqlContext.experimental.extraStrategies =
    (new DruidStrategy(this) +: sqlContext.experimental.extraStrategies)

  val transforms : Seq[Function[(DruidQueryBuilder,LogicalPlan),
    Option[Option[DruidQueryBuilder]]]] = Seq(
    druidRelationTransform.lift,
    aggregateTransform.lift
  )

  def plan(db : DruidQueryBuilder, plan: LogicalPlan): Option[DruidQueryBuilder] = {
    val iter = transforms.view.flatMap(_(db, plan)).toIterator
    if (iter.isEmpty) None else iter.next()
  }

}

object DruidPlanner {

  def apply(sqlContext : SQLContext) = new DruidPlanner(sqlContext)
}
