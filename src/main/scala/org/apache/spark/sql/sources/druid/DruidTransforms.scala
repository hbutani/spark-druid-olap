package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.LogicalRelation
import org.sparklinedata.druid.{DruidRelation, DruidQueryBuilder}


abstract class DruidTransforms {
  self : DruidPlanner =>

  type DruidTransform = PartialFunction[LogicalPlan, DruidQueryBuilder]

  val druidRelationTransform : DruidTransform = {
    case PhysicalOperation(projectList, filters,
    l @ LogicalRelation(d @ DruidRelation(info, None))) =>
      new DruidQueryBuilder(info)
  }

}
