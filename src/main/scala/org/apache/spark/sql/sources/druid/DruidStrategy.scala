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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project => LProject}
import org.apache.spark.sql.execution.{PhysicalRDD, Project, SparkPlan, Union}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ExprUtil
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.{DruidDataType, NonAggregateQueryHandling}
import org.sparklinedata.druid.query.QuerySpecTransforms

private[druid] class DruidStrategy(val planner: DruidPlanner) extends Strategy
  with PredicateHelper with DruidPlannerHelper with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l => {

      val p: Seq[SparkPlan] = for (dqb <- planner.plan(null, l)
      ) yield {
        if (dqb.aggregateOper.isDefined ) {
          aggregatePlan(dqb)
        } else if (dqb.drInfo.options.nonAggQueryHandling !=
          NonAggregateQueryHandling.PUSH_NONE) {
          selectPlan(dqb, l)
        } else {
          null
        }
      }

      val pL = p.filter(_ != null).toList
      if (pL.size < 2) pL else Seq(Union(pL))

    }
  }

  private def selectPlan(dqb : DruidQueryBuilder,
                         l : LogicalPlan) : SparkPlan =  l match {
    case LProject(projectList, _) => selectPlan(dqb, projectList)
    case _ => null
  }

  private def selectPlan(dqb : DruidQueryBuilder,
                         projectList: Seq[NamedExpression]) : SparkPlan = {

    var dqb1 = dqb

    /*
     * 1. Setup the dqb outputAttribute list by adding every AttributeRef
      * from the projectionList
     */
    for(
      p <- projectList ;
      a <- p.references;
      dc <- dqb1.referencedDruidColumns.get(a.name)
    ) {
      dqb1 =
        dqb1.outputAttribute(a.name, a, a.dataType, DruidDataType.sparkDataType(dc.dataType), null)
    }

    val druidOpSchema = new DruidOperatorSchema(dqb1)

    val (dims, metrics) = dqb1.referencedDruidColumns.values.partition(_.isDimension())

    /*
     * 2. Interval is either in the SQL, or use the entire datasource interval
     */
    val intervals = dqb1.queryIntervals.get

    /*
     * 3. Setup SelectQuerySpec
     */
    var qs: QuerySpec = new SelectSpecWithIntervals(
      dqb1.drInfo.druidDS.name,
      dims.map(_.name).toList,
      metrics.map(_.name).toList,
      dqb1.filterSpec,
      PagingSpec(Map(),
        DruidPlanner.getConfValue(planner.sqlContext,
          DruidPlanner.DRUID_SELECT_QUERY_PAGESIZE)
      ),
      intervals.map(_.toString),
      false
    )

    val (queryHistorical : Boolean, numSegsPerQuery : Int) =
      (dqb.drInfo.options.queryHistoricalServers(planner.sqlContext),
        dqb.drInfo.options.numSegmentsPerHistoricalQuery(planner.sqlContext)
        )

    /*
     * 5. Setup DruidRelation
     */
    val dq = DruidQuery(qs,
      dqb.drInfo.options.useSmile(planner.sqlContext),
      queryHistorical,
      numSegsPerQuery,
      intervals,
      Some(druidOpSchema.operatorDruidAttributes))

    buildPlan(
      dqb1,
      druidOpSchema,
      dq,
      planner,
      {druidPhysicalOp =>
        druidPhysicalOp
      },
      {(dqb, druidOpSchema) =>
        buildProjectionList(projectList, druidOpSchema)
      }
    )
  }

  private def aggregatePlan(dqb : DruidQueryBuilder) : SparkPlan = {
    val druidOpSchema = new DruidOperatorSchema(dqb)
    val pAgg = new PostAggregate(druidOpSchema)

    /*
     * 2. Interval is either in the SQL, or use the entire datasource interval
     */
    val intervals = dqb.queryIntervals.get

    /*
     * 3. Setup GroupByQuerySpec
     */
    var qs: QuerySpec = new GroupByQuerySpec(dqb.drInfo.druidDS.name,
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

    val (queryHistorical : Boolean, numSegsPerQuery : Int) =
      if ( !pAgg.canBeExecutedInHistorical ) {
        (false, -1)
      } else if (planner.sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_ENABLED)) {
        val dqc = DruidQueryCostModel.computeMethod(
          planner.sqlContext,
          dqb.drInfo,
          qs
        )
        (dqc.queryHistorical, dqc.numSegmentsPerQuery)
      } else {
        (dqb.drInfo.options.queryHistoricalServers(planner.sqlContext),
          dqb.drInfo.options.numSegmentsPerHistoricalQuery(planner.sqlContext)
          )
      }

    /*
     * 5. Setup DruidRelation
     */
    val dq = DruidQuery(qs,
      dqb.drInfo.options.useSmile(planner.sqlContext),
      queryHistorical,
      numSegsPerQuery,
      intervals,
      Some(druidOpSchema.operatorDruidAttributes))

    buildPlan(
      dqb,
      druidOpSchema,
      dq,
      planner, { druidPhysicalOp =>
        if (dq.queryHistoricalServer) {
          // add an Agg on top of druidQuery
          pAgg.aggOp(druidPhysicalOp).head
        } else {
          druidPhysicalOp
        }
      },
      {(dqb, druidOpSchema) =>
        buildProjectionList(dqb.aggregateOper.get,
          dqb.aggExprToLiteralExpr, druidOpSchema)
      }
    )
  }

  private def buildPlan(
                         dqb : DruidQueryBuilder,
                         druidOpSchema : DruidOperatorSchema,
                         dq : DruidQuery,
                         planner : DruidPlanner,
                         postDruidStep : SparkPlan => SparkPlan,
                         buildProjectionList :
                         (DruidQueryBuilder, DruidOperatorSchema) => Seq[NamedExpression]
                       ) : SparkPlan = {

    planner.debugTranslation(
      s"""
         | DruidQuery:
         |   ${Utils.queryToString(dq)}
                """.stripMargin

    )
    val dR: DruidRelation = DruidRelation(dqb.drInfo, Some(dq))(planner.sqlContext)

    var druidPhysicalOp : SparkPlan = PhysicalRDD.createFromDataSource(
      druidOpSchema.operatorSchema,
      dR.buildInternalScan,
      dR)

    druidPhysicalOp = postDruidStep(druidPhysicalOp)

    if ( druidPhysicalOp != null ) {
      val projections = buildProjectionList(dqb, druidOpSchema)
      Project(projections, druidPhysicalOp)
    } else null
  }

  private def buildProjectionList(aggOp: Aggregate,
                                  grpExprToFillInLiteralExpr: Map[Expression, Expression],
                                  druidOpSchema : DruidOperatorSchema):
  Seq[NamedExpression] = {

    /*
     * Replace aggregationExprs with fillIn expressions setup for this GroupingSet.
     * These are for Grouping__Id and for grouping expressions that are missing(null) for
     * this Grouping Set.
     */
    val aEs = aggOp.aggregateExpressions.map { aE => grpExprToFillInLiteralExpr.getOrElse(aE, aE) }

    buildProjectionList(aEs, druidOpSchema)
  }

  private def buildProjectionList(origExpressions : Seq[Expression],
                                  druidOpSchema : DruidOperatorSchema):
  Seq[NamedExpression] = {

    val druidPushDownExprMap = druidOpSchema.pushedDownExprToDruidAttr
    val avgExpressions = druidOpSchema.avgExpressions

    origExpressions.map(aE => ExprUtil.transformReplace( aE, {
      case e: Expression if avgExpressions.contains(e) => {
        val (s,c) = avgExpressions(e)
        val (sDAttr, cDAttr) = (druidOpSchema.druidAttrMap(s), druidOpSchema.druidAttrMap(c))
        Cast(
          Divide(
            Cast(AttributeReference(sDAttr.name, sDAttr.dataType)(sDAttr.exprId), DoubleType),
            Cast(AttributeReference(cDAttr.name, cDAttr.dataType)(cDAttr.exprId), DoubleType)
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
    })).asInstanceOf[Seq[NamedExpression]]

  }
}
