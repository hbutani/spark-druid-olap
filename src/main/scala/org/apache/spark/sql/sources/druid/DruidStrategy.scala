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

import org.apache.spark.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.{PhysicalRDD, Project, SparkPlan, Union}
import org.apache.spark.sql.types.DoubleType
import org.sparklinedata.druid._
import org.sparklinedata.druid.query.QuerySpecTransforms

private[druid] class DruidStrategy(val planner: DruidPlanner) extends Strategy
with PredicateHelper with DruidPlannerHelper with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l => {

      val p: Seq[SparkPlan] = for (dqb <- planner.plan(null, l);
                                   a <- dqb.aggregateOper
      ) yield {

        val druidOpSchema = new DruidOperatorSchema(dqb)

          /*
           * 2. Interval is either in the SQL, or use the entire datasource interval
           */
          val intervals = dqb.queryIntervals.get

          /*
           * 3. Setup GroupByQuerySpec
           */
          var qs : QuerySpec = new GroupByQuerySpec(dqb.drInfo.druidDS.name,
            dqb.dimensions,
            dqb.limitSpec,
            dqb.havingSpec,
            dqb.granularitySpec,
            dqb.filterSpec,
            dqb.aggregations,
            dqb.postAggregations,
            intervals.map(_.toString)
          )

          /*
           * 4. apply QuerySpec transforms
           */
          qs = QuerySpecTransforms.transform(dqb.drInfo, qs)

          /*
           * 5. Setup DruidRelation
           */
          val dq = DruidQuery(qs, intervals, Some(druidOpSchema.operatorDruidAttributes))

        planner.debugTranslation(
          s"""
             | DruidQuery:
             |   ${Utils.queryToString(dq)}
                """.stripMargin

        )

          val dR: DruidRelation = DruidRelation(dqb.drInfo, Some(dq))(planner.sqlContext)

          val projections = buildProjectionList(dqb.aggregateOper.get,
            dqb.aggExprToLiteralExpr, druidOpSchema)
          Project(projections, PhysicalRDD.createFromDataSource(
            druidOpSchema.operatorSchema,
            dR.buildInternalScan,
            dR))
        }
      val pL = p.toList
      if (pL.size < 2) pL else Seq(Union(pL))

    }
  }

  def buildProjectionList(aggOp: Aggregate,
                          grpExprToFillInLiteralExpr: Map[Expression, Expression],
                          druidOpSchema : DruidOperatorSchema):
  Seq[NamedExpression] = {

    /*
     * Replace aggregationExprs with fillIn expressions setup for this GroupingSet.
     * These are for Grouping__Id and for grouping expressions that are missing(null) for
     * this Grouping Set.
     */
    val aEs = aggOp.aggregateExpressions.map { aE => grpExprToFillInLiteralExpr.getOrElse(aE, aE) }

    val druidPushDownExprMap = druidOpSchema.pushedDownExprToDruidAttr
    val avgExpressions = druidOpSchema.avgExpressions

    aEs.map(_.transformUp {
      case e: Expression if avgExpressions.contains(e) => {
        val (s,c) = avgExpressions(e)
        val (sDAttr, cDAttr) = (druidOpSchema.druidAttrMap(s), druidOpSchema.druidAttrMap(c))
        Cast(
        Divide(
          Cast(AttributeReference(sDAttr.name, sDAttr.dataType)(sDAttr.exprId), DoubleType),
          Cast(AttributeReference(cDAttr.name, sDAttr.dataType)(cDAttr.exprId), DoubleType)
        ),
          e.dataType)
      }
      case ne: AttributeReference if druidPushDownExprMap.contains(ne) &&
        druidPushDownExprMap(ne).dataType != ne.dataType => {
        val dA = druidPushDownExprMap(ne)
        Alias(Cast(
          AttributeReference(dA.name, dA.dataType)(dA.exprId), ne.dataType), dA.name)(dA.exprId)
      }
      case ne: AttributeReference if druidPushDownExprMap.contains(ne) &&
        druidPushDownExprMap(ne).name != ne.name => {
        val dA = druidPushDownExprMap(ne)
        Alias(AttributeReference(dA.name, dA.dataType)(dA.exprId), dA.name)(dA.exprId)
      }
      case ne: AttributeReference if druidPushDownExprMap.contains(ne) => {
        ne
      }
      case e: Expression if druidPushDownExprMap.contains(e) &&
        druidPushDownExprMap(e).dataType != e.dataType => {
        val dA = druidPushDownExprMap(e)
        Cast(
          AttributeReference(dA.name, dA.dataType)(dA.exprId), e.dataType)
      }
      case e: Expression if druidPushDownExprMap.contains(e)
      => {
        val dA = druidPushDownExprMap(e)
        AttributeReference(dA.name, dA.dataType)(dA.exprId)
      }

      case e => e
    }).asInstanceOf[Seq[NamedExpression]]

  }
}
