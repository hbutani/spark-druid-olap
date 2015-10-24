/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression, Expression}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ExtractEquiJoinKeys}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.sources.{LogicalRelation, BaseRelation}
import org.sparklinedata.druid.DruidQueryBuilder

trait JoinTransform {
  self: DruidPlanner =>

  def translateJoin(leftExpressions : Seq[Expression],
                    rightExpressions : Seq[Expression],
                    factTable : LogicalPlan,
                    dimProjectList : Seq[NamedExpression],
                    dimFilters : Seq[Expression],
                    dimRelation : BaseRelation,
                    projectListOnTopOfJoin: Seq[NamedExpression]
                     ) : Seq[DruidQueryBuilder] = {
    joinPlan(null, factTable).flatMap { dqb =>

      var dqb1: Option[DruidQueryBuilder] = Some(dqb)

      val (leftTable, righTable) = dqb1.flatMap{ dqb =>
        dqb.drInfo.starSchema.isStarJoin(leftExpressions, rightExpressions)
      }.getOrElse((null, null))

      dqb1 = leftTable match {
        case null => None
        case _ => dqb1
      }

      val rightJoinAttrs : Set[String] = rightExpressions.map {
        case AttributeReference(nm, _, _, _) => nm
      }.toSet

      translateProjectFilter(dqb1,
        dimProjectList,
        dimFilters,
        rightJoinAttrs)

    }
  }

  val joinTransform: DruidTransform = {
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    left,
    PhysicalOperation(projectList, filters, l@LogicalRelation(dimRelation))
    )
      ) if (joinPlan(null, left).size > 0 ) => {
      /*
       * TODO: not very efficient way to decide that left subtree contains the factTable.
       */

      translateJoin(leftExpressions,
        rightExpressions,
        left,
        projectList,
        filters,
        dimRelation,
        Seq()
      )
    }
    case (dqb, Project(joinProjectList,
    ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    left,
    PhysicalOperation(projectList, filters, l@LogicalRelation(dimRelation))
    )
    )) if (joinPlan(null, left).size > 0 ) => {

      translateJoin(leftExpressions,
        rightExpressions,
        left,
        projectList,
        filters,
        dimRelation,
        joinProjectList
      )
    }
    /*
     * for the next 2 cases left & right, so that translateJoin called as though the
     * join was starSchemaSoFar join dimTable
     */
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    rightExpressions,
    leftExpressions,
    None,
    PhysicalOperation(projectList, filters, l@LogicalRelation(dimRelation)),
    left
    )
      ) => {

      translateJoin(leftExpressions,
        rightExpressions,
        left,
        projectList,
        filters,
        dimRelation,
        Seq()
      )
    }
    case (dqb, Project(joinProjectList,
    ExtractEquiJoinKeys(
    Inner,
    rightExpressions,
    leftExpressions,
    None,
    PhysicalOperation(projectList, filters, l@LogicalRelation(dimRelation)),
    left
    )
    )) => {

      translateJoin(leftExpressions,
        rightExpressions,
        left,
        projectList,
        filters,
        dimRelation,
        joinProjectList
      )
    }
    case _ => Seq()
  }

}

