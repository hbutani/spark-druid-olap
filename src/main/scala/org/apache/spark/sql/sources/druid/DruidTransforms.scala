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

import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.{DruidColumn, DruidDataType, DruidDimension, DruidMetric}

import scala.collection.mutable.ArrayBuffer

abstract class DruidTransforms extends DruidPlannerHelper {
  self: DruidPlanner =>

  type DruidTransform = Function[(Seq[DruidQueryBuilder], LogicalPlan), Seq[DruidQueryBuilder]]
  type ODB = Option[DruidQueryBuilder]

  val druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l@LogicalRelation(d@DruidRelation(info, None)))) => {
      var dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))
      dqb = projectList.foldLeft(dqb) { (dqB, e) =>
        dqB.flatMap(projectExpression(_, e))
      }

      if (dqb.isDefined) {
        /*
         * Filter Rewrites:
         * - A conjunct is a predicate on the Time Dimension => rewritten to Interval constraint
         * - A expression containing comparisons on Dim Columns.
         */
        val iCE: IntervalConditionExtractor = new IntervalConditionExtractor(dqb.get)
        filters.foldLeft(dqb) { (dqB, e) =>
          dqB.flatMap { b =>
            intervalFilterExpression(b, iCE, e).orElse(
              dimFilterExpression(b, e).map(p => b.filter(p))
            )
          }
        }.map(Seq(_)).getOrElse(Seq())
      } else Seq()
    }
    case _ => Seq()
  }

  private def transformSingleGrouping(dqb: DruidQueryBuilder,
                                      aggOp: Aggregate,
                                      grpInfo: GroupingInfo): Option[DruidQueryBuilder] = {
    val timeElemExtractor = new TimeElementExtractor(dqb)

    /*
     * For pushing down to Druid only consider the GrpExprs that don't have a override
     * for this GroupingInfo(these are expressions that are missing(null) and the Grouping__Id
     * column). For the initial call, the grouping__id is not in the override list, so we
     * also check for it explicitly here.
     */
    val gEsToDruid = grpInfo.gEs.filter {
      case x if grpInfo.aEToLiteralExpr.contains(x) => false
      case AttributeReference(
      VirtualColumn.groupingIdName, IntegerType, _, _) => false
      case _ => true
    }

    val dqb1 =
      gEsToDruid.foldLeft(Some(dqb).asInstanceOf[Option[DruidQueryBuilder]]) {
        (dqb, e) =>
          dqb.flatMap(groupingExpression(_, timeElemExtractor, e))
      }

    val allAggregates =
      grpInfo.aEs.flatMap(_ collect { case a: AggregateExpression => a })
    // Collect all aggregate expressions that can be computed partially.
    val partialAggregates =
      grpInfo.aEs.flatMap(_ collect { case p: PartialAggregate => p })

    // Only do partial aggregation if supported by all aggregate expressions.
    if (allAggregates.size == partialAggregates.size) {
      val dqb2 = partialAggregates.foldLeft(dqb1) {
        (dqb, ne) =>
          dqb.flatMap(aggregateExpression(_, ne))
      }

      dqb2.map(_.aggregateOp(aggOp)).map(
        _.copy(aggExprToLiteralExpr = grpInfo.aEToLiteralExpr)
      )
    } else {
      None
    }
  }


  val aggregateTransform: DruidTransform = {
    case (dqb, agg@Aggregate(gEs, aEs,
    p@Project(projectList, expandOp@Expand(projections, output, child)))) => {
      plan(dqb, child).flatMap { dqb =>
        /*
         * First check if the GroupBy and Aggregate Expressions are pushable to
         * Druid.
         */
        val dqb1 = transformSingleGrouping(dqb,
          agg,
          GroupingInfo(gEs, aEs))

        /*
         * If yes, then try to create a DruidQueryBuilder for each Expand projection.
         * Currently this is a all or nothing step. If any projection is not
         * rewritten to a DQB then the entire rewrite is abandoned.
         */
        val childDQBs: Option[Seq[DruidQueryBuilder]] = dqb1.flatMap { _ =>

          /*
           * Map from a GroupExpression to the index in the Expand.projections
           */
          val attrRefIdxList: Option[List[(Expression, (AttributeReference, Int))]] =
            Utils.sequence(gEs.map(gE => positionOfAttribute(gE, expandOp)).toList)

          attrRefIdxList.map { attrRefIdxList =>

            val geToPosMap = attrRefIdxList.toMap

            val grpSetInfos = projections.map { p =>
              /*
               * Represents a mapping from an AggExpr to the override value added in the Project
               * above the DruidRDD. Any missing(null) Group Expr for this Projection and
               * the Grouping__Id columns are added to this list. So these are not pushed to
               * Druid, but added in on the Projection above the Druid ResultSet.
               */
              val literlVals: ArrayBuffer[(Expression, Expression)] = ArrayBuffer()

              /*
               * a list of gE and its corresponding aE. But for grouping___id, it may not
               * be in aE list, just use the gE.
               */
              val gEsAndaEs = gEs.init.zip(aEs) :+(gEs.last, gEs.last)

              gEsAndaEs.foreach { t =>
                val gE: Expression = t._1
                val aE: Expression = t._2
                val gEAttrRef = geToPosMap(gE)._1
                val gExprDT = gEAttrRef.dataType
                val idx = geToPosMap(gE)._2
                val grpingExpr = p.children(idx)

                val transformedGExpr: Expression = aE match {
                  case ar@AttributeReference(nm, _, _, _) => Alias(grpingExpr, nm)(ar.exprId)
                  case al@Alias(_, nm) => Alias(grpingExpr, nm)(al.exprId)
                  case _ => gE.transformUp {
                    case x if x == gEAttrRef => grpingExpr
                  }
                }

                (aE, grpingExpr) match {
                  case (AttributeReference(
                  VirtualColumn.groupingIdName, IntegerType, _, _), _) =>
                    literlVals += ((aE, transformedGExpr))
                  case (Alias(AttributeReference(
                  VirtualColumn.groupingIdName, IntegerType, _, _), _), _) =>
                    literlVals += ((aE, transformedGExpr))
                  case (_, Literal(null, gExprDT)) => literlVals += ((aE, transformedGExpr))
                  case _ => ()
                }

              }

              GroupingInfo(gEs, aEs, literlVals.toMap)
            }

            val dqbs: Seq[Option[DruidQueryBuilder]] = grpSetInfos.map { gInfo =>

              transformSingleGrouping(dqb,
                agg,
                gInfo)

            }
            Utils.sequence(dqbs.toList).getOrElse(Nil)
          }

        }
        childDQBs.getOrElse(Nil)
      }
    }
    case (dqb, agg@Aggregate(gEs, aEs, child)) => {
      plan(dqb, child).flatMap { dqb =>
        transformSingleGrouping(dqb,
          agg,
          GroupingInfo(gEs, aEs))
      }
    }
    case _ => Seq()
  }


  def aggregateExpression(dqb: DruidQueryBuilder, pa: PartialAggregate):
  Option[DruidQueryBuilder] = pa match {
    case Count(Literal(1, IntegerType)) => {
      val a = dqb.nextAlias
      Some(dqb.aggregate(FunctionAggregationSpec("count", a, "count")).
        outputAttribute(a, pa, pa.dataType, LongType))
    }
    case c => {
      val a = dqb.nextAlias
      (dqb, c) match {
        case CountDistinctAggregate(dN) =>
          Some(dqb.aggregate(new CardinalityAggregationSpec(a, List(dN))).
            outputAttribute(a, pa, pa.dataType, DoubleType))
        case SumMinMaxAvgAggregate(t) => t._1 match {
          case "avg" => {
            val dC: DruidColumn = t._2
            val aggFunc = dC.dataType match {
              case DruidDataType.Long => "longSum"
              case _ => "doubleSum"
            }
            val sumAlias = dqb.nextAlias
            val countAlias = dqb.nextAlias

            Some(
              dqb.aggregate(FunctionAggregationSpec(aggFunc, sumAlias, dC.name)).
                aggregate(FunctionAggregationSpec("count", countAlias, "count")).
                postAggregate(new ArithmeticPostAggregationSpec(a, "/",
                List(new FieldAccessPostAggregationSpec(sumAlias),
                  new FieldAccessPostAggregationSpec(countAlias)), None)).
                outputAttribute(a, pa, pa.dataType, DoubleType)
            )
          }
          case _ => {
            val dC: DruidColumn = t._2
            Some(dqb.aggregate(FunctionAggregationSpec(t._1, a, dC.name)).
              outputAttribute(a, pa, pa.dataType, DruidDataType.sparkDataType(dC.dataType))
            )
          }
        }
        case _ => None
      }
    }
  }

  private def attributeRef(arg: Expression): Option[String] = arg match {
    case AttributeReference(name, _, _, _) => Some(name)
    case Cast(AttributeReference(name, _, _, _), _) => Some(name)
    case _ => None
  }

  private object CountDistinctAggregate {
    def unapply(t: (DruidQueryBuilder, PartialAggregate)): Option[(String)]
    = {
      val dqb = t._1
      val pa = t._2

      val r = for (c <- pa.children.headOption if (pa.children.size == 1);
                   dNm <- attributeRef(c);
                   dD <-
                   dqb.druidColumn(dNm) if dD.isInstanceOf[DruidDimension]
      ) yield (pa, dD)

      r flatMap {
        case (p: CountDistinct, dD) if dqb.drInfo.allowCountDistinct => Some(dD.name)
        case (p: ApproxCountDistinct, dD) if dqb.drInfo.allowCountDistinct => Some(dD.name)
        case _ => None
      }
    }
  }

  /**
   * 1. PartialAgg must have only 1 arg.
   * 2. The arg must be an AttributeRef or a Cast of an AttributeRef
   * 3. Attribute must be a DruidMetric.
   * 4. The dataType of the PartialAgg must match the DruidMetric DataType, except for Avg.
   */
  private object SumMinMaxAvgAggregate {

    def unapply(t: (DruidQueryBuilder, PartialAggregate)): Option[(String, DruidColumn)]
    = {
      val dqb = t._1
      val pa = t._2

      val r = for (c <- pa.children.headOption if (pa.children.size == 1);
                   mNm <- attributeRef(c);
                   dM <- dqb.druidColumn(mNm) if dM.isInstanceOf[DruidMetric];
                   mDT <- Some(DruidDataType.sparkDataType(dM.dataType));
                   commonType <- HiveTypeCoercion.findTightestCommonType(pa.dataType, mDT)
                   if (commonType == mDT || pa.isInstanceOf[Average])
      ) yield (pa, commonType, dM)

      r flatMap {
        case (p: Sum, LongType, dM) => Some(("longSum", dM))
        case (p: Sum, DoubleType, dM) => Some(("doubleSum", dM))
        case (p: Min, LongType, dM) => Some(("longMin", dM))
        case (p: Min, DoubleType, dM) => Some(("doubleMin", dM))
        case (p: Max, LongType, dM) => Some(("longMax", dM))
        case (p: Max, DoubleType, dM) => Some(("doubleMax", dM))
        case (p: Average, LongType, dM) => Some(("avg", dM))
        case (p: Average, DoubleType, dM) => Some(("avg", dM))
        case _ => None
      }
    }


  }

  /**
   * Match the following as a rewritable Grouping Expression:
   * - an AttributeReference, these are translated to a [[DefaultDimensionSpec]]
   * - a [[TimeElementExtractor]] expression, these are translated to [[ExtractionDimensionSpec]]
   * with a [[TimeFormatExtractionFunctionSpec]]
   * @param dqb
   * @param timeElemExtractor
   * @param ge
   * @return
   */
  def groupingExpression(dqb: DruidQueryBuilder,
                         timeElemExtractor: TimeElementExtractor,
                         ge: Expression):
  Option[DruidQueryBuilder] = {
    ge match {
      case AttributeReference(nm, dT, _, _) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield dqb.dimension(new DefaultDimensionSpec(dD.name, nm)).
            outputAttribute(nm, ge, ge.dataType, DruidDataType.sparkDataType(dD.dataType))
      }
      case timeElemExtractor(nm, dC, tzId, fmt) => {
        Some(
          dqb.dimension(new ExtractionDimensionSpec(dC.name, nm,
            new TimeFormatExtractionFunctionSpec(fmt, tzId))).
            outputAttribute(nm, ge, ge.dataType, StringType)
        )
      }
      case _ => None
    }
  }

  def projectExpression(dqb: DruidQueryBuilder, pe: Expression):
  Option[DruidQueryBuilder] = pe match {
    case AttributeReference(nm, dT, _, _) if dqb.druidColumn(nm).isDefined
    => Some(dqb)
    case Alias(ar@AttributeReference(nm1, dT, _, _), nm) => {
      for (dqbc <- projectExpression(dqb, ar))
        yield dqbc.addAlias(nm, nm1)
    }
    case _ => None
  }

  def intervalFilterExpression(dqb: DruidQueryBuilder,
                               iCE: IntervalConditionExtractor, fe: Expression):
  Option[DruidQueryBuilder] = fe match {
    case iCE(iC) => dqb.interval(iC)
    case _ => None
  }

  def dimFilterExpression(dqb: DruidQueryBuilder, fe: Expression):
  Option[FilterSpec] = {

    val dtTimeCond = new DateTimeConditionExtractor(dqb)

    fe match {
      case EqualTo(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield new SelectorFilterSpec(dD.name, value.toString)
      }
      case EqualTo(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield new SelectorFilterSpec(dD.name, value.toString)
      }
      case LessThan(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<", value.toString)
      }
      case LessThan(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">", value.toString)
      }
      case LessThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<=", value.toString)
      }
      case LessThanOrEqual(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">=", value.toString)
      }

      case GreaterThan(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">", value.toString)
      }
      case GreaterThan(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<", value.toString)
      }
      case GreaterThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">=", value.toString)
      }
      case GreaterThanOrEqual(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<=", value.toString)
      }
      case dtTimeCond((dCol, op, value)) =>
        Some(JavascriptFilterSpec.create(dCol, op, value))
      case Or(e1, e2) => {
        Utils.sequence(
          List(dimFilterExpression(dqb, e1), dimFilterExpression(dqb, e2))).map { args =>
          LogicalFilterSpec("or", args.toList)
        }
      }
      case And(e1, e2) => {
        Utils.sequence(
          List(dimFilterExpression(dqb, e1), dimFilterExpression(dqb, e2))).map { args =>
          LogicalFilterSpec("and", args.toList)
        }
      }
      case _ => None
    }
  }

  /**
   * ==Sort Rewrite:==
   * A '''Sort''' Operator is pushed down to ''Druid'' if all its __order expressions__
   * can be pushed down. An __order expression__ is pushed down if it is on an ''Expression''
   * that is already pushed to Druid, or if it is an [[Alias]] expression whose child
   * has been pushed to Druid.
   *
   * ==Limit Rewrite:==
   * A '''Limit''' Operator above a Sort is always pushed down to Druid. The __limit__
   * value is set on the [[LimitSpec]] of the [[GroupByQuerySpec]]
   */
  val limitTransform: DruidTransform = {
    case (dqb, sort@Sort(orderExprs, global, child: Aggregate)) => {
      // TODO: handle Having
      val dqbs = plan(dqb, child).map { dqb =>
        val exprToDruidOutput =
          buildDruidSchemaMap(dqb.outputAttributeMap)

        val dqb1: ODB = orderExprs.foldLeft(Some(dqb).asInstanceOf[ODB]) { (dqb, e) =>
          for (ue <- unalias(e.child, child);
               doA <- exprToDruidOutput.get(ue))
            yield dqb.get.orderBy(doA.name, e.direction == Ascending)
        }
        dqb1
      }
      Utils.sequence(dqbs.toList).getOrElse(Seq())
    }
    case (dqb, sort@Limit(limitExpr, child: Sort)) => {
      val dqbs = plan(dqb, child).map { dqb =>
        val amt = limitExpr.eval(null).asInstanceOf[Int]
        dqb.limit(amt)
      }
      Utils.sequence(dqbs.toList).getOrElse(Seq())
    }
    case _ => Seq()
  }

  val joinTransform : DruidTransform = {
      case (dqb, Join(
      left,
        PhysicalOperation(projectList, filters,l@LogicalRelation(dimRelation)),
     Inner,
      Some(joinCond)
      )
        )=> {

        joinPlan(dqb, left).flatMap { dqb =>

          var dqb1: Option[DruidQueryBuilder] = Some(dqb)

          dqb1 = projectList.foldLeft(dqb1) { (dqB, e) =>
            dqB.flatMap(projectExpression(_, e))
          }

          if (dqb1.isDefined) {
            /*
             * Filter Rewrites:
             * - A conjunct is a predicate on the Time Dimension => rewritten to Interval constraint
             * - A expression containing comparisons on Dim Columns.
             */
            val iCE: IntervalConditionExtractor = new IntervalConditionExtractor(dqb1.get)
            filters.foldLeft(dqb1) { (dqB, e) =>
              dqB.flatMap { b =>
                intervalFilterExpression(b, iCE, e).orElse(
                  dimFilterExpression(b, e).map(p => b.filter(p))
                )
              }
            }.map(Seq(_)).getOrElse(Seq())
          } else Seq()
        }

      }
      case _ => Seq()
    }
}
