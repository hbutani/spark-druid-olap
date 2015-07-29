package org.apache.spark.sql.sources.druid

import org.apache.spark.Logging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan, Project}
import org.apache.spark.sql.types.DataType
import org.sparklinedata.druid._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

private[druid] class DruidStrategy(val planner: DruidPlanner) extends Strategy
with PredicateHelper with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l => {

      val p = for (dqb <- planner.plan(null, l);
                   a <- dqb.aggregateOper
      ) yield {

          /*
           * 1. build the map from aggOp expressions to DruidOperatorAttribute
           */
          val exprToDruidOutput =
            buildDruidSchemaMap(dqb.outputAttributeMap)

          /*
           * 2. Interval is either in the SQL, or use the entire datasource interval
           */
          val intervals = dqb.queryIntervals.get

          /*
           * 3. Setup GroupByQuerySpec
           */
          val qs = new GroupByQuerySpec(dqb.drInfo.druidDS.name,
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
           * 4. Setup DruidRelation
           */
          val dq = DruidQuery(qs, intervals, Some(exprToDruidOutput.values.toList))

          //Utils.logQuery(dq)

          val dR: DruidRelation = DruidRelation(dqb.drInfo, Some(dq))(planner.sqlContext)

          /*
           * 5. Setup SparkPlan = PhysicalRDD + Projection
           */
          val druidOpAttrs = exprToDruidOutput.values.map {
            case DruidOperatorAttribute(eId, nm, dT) => AttributeReference(nm, dT)(eId)
          }
          val projections = buildProjectionList(dqb.aggregateOper.get, exprToDruidOutput)
          Project(projections, new PhysicalRDD(druidOpAttrs.toList, dR.buildScan()))
        }

      p.map(List(_)).getOrElse(Nil)

    }
  }

  /**
   *
   * @param outputAttributeMap outputAttributeMap from the QryBldr
   * @return a map from the AggregationOp output to a DruidOperatorAttribute
   */
  def buildDruidSchemaMap(outputAttributeMap:
                                    Map[String, (Expression, DataType, DataType)]):
  Map[Expression, DruidOperatorAttribute] = (outputAttributeMap map {
    case (nm, (e, oDT, dDT)) => {
      val druidEid = e match {
        case n: NamedExpression => n.exprId
        case _ => NamedExpression.newExprId
      }
      (e -> DruidOperatorAttribute(druidEid,nm, dDT))
    }
  })


  def buildProjectionList(aggOp : Aggregate,
                          druidPushDownExprMap : Map[Expression, DruidOperatorAttribute]) :
  Seq[NamedExpression] = {

    aggOp.aggregateExpressions.map(_.transformUp {
      case ne: AttributeReference if druidPushDownExprMap.contains(ne)  &&
        druidPushDownExprMap(ne).dataType != ne.dataType => {
        val dA = druidPushDownExprMap(ne)
        Alias(Cast(
          AttributeReference(dA.name, dA.dataType)(dA.exprId), ne.dataType), dA.name)(dA.exprId)
      }
      case ne: AttributeReference if druidPushDownExprMap.contains(ne)  &&
        druidPushDownExprMap(ne).name != ne.name => {
        val dA = druidPushDownExprMap(ne)
        Alias(AttributeReference(dA.name, dA.dataType)(dA.exprId), dA.name)(dA.exprId)
      }
      case ne: AttributeReference if druidPushDownExprMap.contains(ne)  => {
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
