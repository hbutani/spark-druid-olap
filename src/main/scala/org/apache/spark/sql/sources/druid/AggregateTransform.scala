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
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Project, Aggregate}
import org.apache.spark.sql.types.{DoubleType, LongType, IntegerType, StringType}
import org.sparklinedata.druid.metadata.{DruidMetric, DruidColumn, DruidDataType, DruidDimension}
import org.sparklinedata.druid._

import scala.collection.mutable.ArrayBuffer

trait AggregateTransform {
  self: DruidPlanner =>

  /**
   * Match the following as a rewritable Grouping Expression:
   * - an AttributeReference, these are translated to a [[DefaultDimensionSpec]]
   * - a [[TimeElementExtractor]] expression, these are translated to [[ExtractionDimensionSpec]]
   * with a [[TimeFormatExtractionFunctionSpec]]
    *
    * @param dqb
   * @param timeElemExtractor
   * @param ge
   * @return
   */
  def groupingExpression(dqb: DruidQueryBuilder,
                         timeElemExtractor: TimeElementExtractor,
                         timeElemExtractor2 : SparkNativeTimeElementExtractor,
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
      case timeElemExtractor2(dtGrpElem) => {
        val timeFmtExtractFunc : ExtractionFunctionSpec = {
          if (dtGrpElem.inputFormat.isDefined) {
            new TimeParsingExtractionFunctionSpec(dtGrpElem.inputFormat.get,
              dtGrpElem.formatToApply)
          } else {
            new TimeFormatExtractionFunctionSpec(dtGrpElem.formatToApply,
              dtGrpElem.tzForFormat)
          }
        }
        Some(
          dqb.dimension(new ExtractionDimensionSpec(dtGrpElem.druidColumn.name,
            dtGrpElem.outputName, timeFmtExtractFunc)
          ).
            outputAttribute(dtGrpElem.outputName, dtGrpElem.pushedExpression,
              dtGrpElem.pushedExpression.dataType, StringType)
        )
      }
      case _ => None
    }
  }
  private def transformSingleGrouping(dqb: DruidQueryBuilder,
                                      aggOp: Aggregate,
                                      grpInfo: GroupingInfo): Option[DruidQueryBuilder] = {
    val timeElemExtractor = new TimeElementExtractor(dqb)
    val timeElemExtractor2 = new SparkNativeTimeElementExtractor(dqb)

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
          dqb.flatMap(groupingExpression(_, timeElemExtractor, timeElemExtractor2, e))
      }

    val allAggregates =
      grpInfo.aEs.flatMap(_ collect { case a: AggregateExpression => a })
    // Collect all aggregate expressions that can be computed partially.
    val partialAggregates =
      grpInfo.aEs.flatMap(_ collect { case p: PartialAggregate1 => p })

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
    expandOp@Expand(bitmasks, _, _, child))) => {
      plan(dqb, child).flatMap { dqb =>

        val projections = expandOp.projections
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
                val grpingExpr = p(idx)

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

  def aggregateExpression(dqb: DruidQueryBuilder, pa: PartialAggregate1):
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
    def unapply(t: (DruidQueryBuilder, PartialAggregate1)): Option[(String)]
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

    def unapply(t: (DruidQueryBuilder, PartialAggregate1)): Option[(String, DruidColumn)]
    = {
      val dqb = t._1
      val pa = t._2

      val r = for (c <- pa.children.headOption if (pa.children.size == 1);
                   mNm <- attributeRef(c);
                   dM <- dqb.druidColumn(mNm) if dM.isInstanceOf[DruidMetric];
                   mDT <- Some(DruidDataType.sparkDataType(dM.dataType));
                   commonType <- HiveTypeCoercion.findTightestCommonTypeOfTwo(pa.dataType, mDT)
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

}


