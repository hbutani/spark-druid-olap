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

import org.apache.spark.sql.SPLLogging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Union, Project => LProject}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.util.ExprUtil
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata._
import org.sparklinedata.druid.query.QuerySpecTransforms

import scala.collection.mutable.{Map => MMap}

private[sql] class DruidStrategy(val planner: DruidPlanner) extends Strategy
  with PredicateHelper with DruidPlannerHelper with SPLLogging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l => {

      val p: Seq[SparkPlan] = for (dqb <- planner.plan(null, l)
      ) yield {
        if (dqb.aggregateOper.isDefined) {
          aggregatePlan(dqb)
        } else {
          dqb.drInfo.options.nonAggQueryHandling match {
            case NonAggregateQueryHandling.PUSH_FILTERS if dqb.filterSpec.isDefined =>
              selectPlan(dqb, l)
            case NonAggregateQueryHandling.PUSH_PROJECT_AND_FILTERS =>
              selectPlan(dqb, l)
            case _ => null
          }
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
     * Add all projectList attributeRefs to DQB; this may have already happened.
     * But in a JoinTree the projectLists are ignored during Join Transform,
     * projections are handled either by the AggregateTransform or they are
     * handled here.
     */
    val attrRefs = for(
      p <- projectList ;
      a <- p.references
    ) yield a

    val odqb = attrRefs.foldLeft(Some(dqb1):Option[DruidQueryBuilder]){(dqb, ar) =>
      dqb.flatMap(dqb => dqb.druidColumn(ar.name).map(_ => dqb))
    }

    if ( !odqb.isDefined ) {
      return null
    } else {
      dqb1 = odqb.get
    }

    /*
     * Druid doesn't allow '__time' to be referenced as a dimension in the SelectQuery dim list.
     * But it does return the timestamp with each ResultRow, so replace '__time'
     * with the key 'timestamp'. Also the timestamp is returned as a iso format String
     * and not an epoch.
     */
    val replaceTimeReferencedDruidColumns = dqb1.referencedDruidColumns.mapValues {
      case dtc if dtc.isTimeDimension => DruidRelationColumn(
        dtc.column,
        Some(DruidDimension(
          DruidDataSource.EVENT_TIMESTAMP_KEY_NAME,
          DruidDataType.String,
          dtc.size,
          dtc.cardinality)),
        dtc.spatialIndex,
        dtc.hllMetric,
        dtc.sketchMetric,
        dtc.cardinalityEstimate
      )
      case dc => dc
    }

    dqb1 = dqb1.copy(referencedDruidColumns = MMap(replaceTimeReferencedDruidColumns.toSeq : _*))

    /*
     * 1. Setup the dqb outputAttribute list by adding every AttributeRef
      * from the projectionList
     */
    for(
      p <- projectList ;
      a <- p.references;
      dc <- dqb1.referencedDruidColumns.get(a.name)
    ) {
      var tfN : String = null
      if (dc.hasSpatialIndex) {
        tfN = DruidValTransform.dimConversion(dc.spatialIndex.get.spatialPosition)
      }
      dqb1 =
        dqb1.outputAttribute(a.name, a, a.dataType, DruidDataType.sparkDataType(dc.dataType), tfN)
    }

    /*
     * add attributes for unpushed filters
     */
    for(
      of <- dqb1.origFilter ;
      a <- of.references;
      dc <- dqb1.referencedDruidColumns.get(a.name)
    ) {
      var tfN : String = null
      if (dc.hasSpatialIndex) {
        tfN = DruidValTransform.dimConversion(dc.spatialIndex.get.spatialPosition)
      }
      dqb1 =
        dqb1.outputAttribute(a.name, a, a.dataType, DruidDataType.sparkDataType(dc.dataType), tfN)
    }

    val druidOpSchema = new DruidOperatorSchema(dqb1)

    var (dims, metrics) = dqb1.referencedDruidColumns.values.partition(
      c => c.isDimension() || c.hasSpatialIndex)

    /*
     * Remove 'timestamp' from the dimension list, this is returned by druid with every ResultRow.
     */
    dims = dims.filter(_.name != DruidDataSource.EVENT_TIMESTAMP_KEY_NAME)

    /*
     * If dims or metrics are empty, arbitararily pick 1 dim and metric.
     * otherwise Druid interprets an empty list as having
     * to return all dimensions/metrics.
     */

    if (dims.isEmpty ) {
      dims = Seq(DruidRelationColumn(dqb1.drInfo.druidDS.dimensions.head))
    }

    if (metrics.isEmpty ) {
      metrics = Seq(DruidRelationColumn(dqb1.drInfo.druidDS.metrics.values.head))
    }

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
      false,
      "all",
      Some(
        QuerySpecContext(
          s"query-${System.nanoTime()}"
        )
      )
    )

    val (queryHistorical : Boolean, numSegsPerQuery : Int) =
      if (planner.sqlContext.conf.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_ENABLED)) {
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

    def addFilters(druidPhysicalOp : SparkPlan) : SparkPlan = {
      dqb1.hasUnpushedFilters match {
        case true if  dqb1.origFilter.isDefined => {
          val druidPushDownExprMap = druidOpSchema.pushedDownExprToDruidAttr
          val f = ExprUtil.transformReplace(dqb1.origFilter.get, {
            case ne: AttributeReference if druidPushDownExprMap.contains(ne) &&
              druidPushDownExprMap(ne).dataType != ne.dataType => {
              val dA = druidPushDownExprMap(ne)
              Cast(
                AttributeReference(dA.name, dA.dataType)(dA.exprId),
                ne.dataType)
            }
          }
          )
          FilterExec(f, druidPhysicalOp)
        }
        case _ => druidPhysicalOp
      }
    }

    buildPlan(
      dqb1,
      druidOpSchema,
      dq,
      planner,
      addFilters _,
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
      intervals.map(_.toString),
      Some(
        QuerySpecContext(
          s"query-${System.nanoTime()}"
        )
      )
    )

    /*
     * 4. apply QuerySpec transforms
     */
    qs = QuerySpecTransforms.transform(planner.sqlContext, dqb.drInfo, qs)

    qs.context.foreach{ ctx =>
      if( planner.sqlContext.conf.getConf(DruidPlanner.DRUID_USE_V2_GBY_ENGINE) ) {
        ctx.groupByStrategy = Some("v2")
      }
    }

    val (queryHistorical : Boolean, numSegsPerQuery : Int) =
      if ( !pAgg.canBeExecutedInHistorical ) {
        (false, -1)
      } else if (planner.sqlContext.conf.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_ENABLED)) {
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

    var druidPhysicalOp : SparkPlan = DataSourceScanExec.create(
      druidOpSchema.operatorSchema,
      dR.buildInternalScan,
      dR)

    druidPhysicalOp = postDruidStep(druidPhysicalOp)

    if ( druidPhysicalOp != null ) {
      val projections = buildProjectionList(dqb, druidOpSchema)
      ProjectExec(projections, druidPhysicalOp)
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
