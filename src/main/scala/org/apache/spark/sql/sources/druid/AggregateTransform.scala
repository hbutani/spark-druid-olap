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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan, Project}
import org.apache.spark.sql.types._
import org.sparklinedata.druid._
import org.sparklinedata.druid.jscodegen.{JSAggGenerator, JSCodeGenerator}
import org.sparklinedata.druid.metadata._

import scala.collection.mutable.ArrayBuffer

trait AggregateTransform {
  self: DruidPlanner =>

  def aggExpressions(aEs : Seq[Expression]) : Seq[AggregateExpression] =
  aEs.flatMap(_ collect { case a: AggregateExpression => a }).distinct

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
                         ge: Expression,
                         expandOpExp : Expression):
  Option[DruidQueryBuilder] = {
    expandOpExp match {
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
      case _ => {
        val codeGen = JSCodeGenerator(dqb, ge, false, false,
          sqlContext.getConf(DruidPlanner.TZ_ID).toString)
        for (fn <- codeGen.fnCode) yield {
          val outDName = dqb.nextAlias(codeGen.fnParams.last)
          dqb.dimension(new ExtractionDimensionSpec(codeGen.fnParams.last, outDName,
            new JavaScriptExtractionFunctionSpec("javascript", fn))).
            outputAttribute(outDName, ge, ge.dataType, StringType)
        }
      }
    }
  }

  private def transformSingleGrouping(dqb: DruidQueryBuilder,
                                      aggOp: Aggregate,
                                      grpInfo: GroupingInfo): Option[DruidQueryBuilder] = {

    debugTranslation(
      s"""
         | Translating GroupingInfo:
         |   ${grpInfo}
                """.stripMargin

    )

    val timeElemExtractor = new TimeElementExtractor(dqb)
    val timeElemExtractor2 = new SparkNativeTimeElementExtractor(dqb)

    implicit val expandOpProjection : Seq[Expression] = grpInfo.expandOpProjection
    implicit val aEExprIdToPos : Map[ExprId, Int] = grpInfo.aEExprIdToPos
    implicit val aEToLiteralExpr: Map[Expression, Expression] = grpInfo.aEToLiteralExpr

    /*
     * For pushing down to Druid only consider the GrpExprs that don't have a override
     * for this GroupingInfo(these are expressions that are missing(null) and the Grouping__Id
     * column). For the initial call, the grouping__id is not in the override list, so we
     * also check for it explicitly here.
     */
    val gEsToDruid = grpInfo.gEs.zip(grpInfo.expandOpGExps).filter {
      case (x,_) if grpInfo.aEToLiteralExpr.contains(x) => false
      case (AttributeReference(
      GroupingColumnName(_), IntegerType, _, _), _) => false
      case _ => true
    }

    debugTranslation(
      s"""
         | Candidate Group Expressions pushed to Druid:
         |   ${gEsToDruid}
                """.stripMargin

    )

    val dqb1 =
      gEsToDruid.foldLeft(Some(dqb).asInstanceOf[Option[DruidQueryBuilder]]) {
        (dqb, e) =>
          dqb.flatMap(groupingExpression(_,
            timeElemExtractor, timeElemExtractor2, e._1, e._2))
      }

    /*
     * if the aE is a null agg for count distinct, ignore it when
     * pushing to Druid.
     */
    val allAggregates = aggExpressions(grpInfo.aEs.filterNot(aEToLiteralExpr.contains(_)))

    debugTranslation(
      s"""
         | Candidate Agg Expressions pushed to Druid:
         |   ${allAggregates}
                """.stripMargin

    )

    val dqb2 = allAggregates.foldLeft(dqb1) {
      (dqb, ne) =>
        dqb.flatMap(aggregateExpression(_, ne))
    }

    dqb2.map(_.aggregateOp(aggOp)).map(
      _.copy(aggExprToLiteralExpr = grpInfo.aEToLiteralExpr)
    )

  }


  val aggregateTransform: DruidTransform = {
    case (dqb,
    AggregateMatch((agg, gEs, aEs, projectList, expandOp, projections, child))) => {
      plan(dqb, child).flatMap { dqb =>

        val dqb1 =Some(dqb)

        /*
         * For each Expand projection.
         * Currently this is a all or nothing step. If any projection is not
         * rewritten to a DQB then the entire rewrite is abandoned.
         */
        val childDQBs: Option[Seq[DruidQueryBuilder]] = dqb1.flatMap { _ =>

          /*
           * For each GroupExpression compute the corresponding Expression in the
           * Expand Output and its position within the Expand project list.
           */
          val oattrRefIdxList: Option[List[(Expression, (AttributeReference, Int))]] =
            Utils.sequence(gEs.map(gE => positionOfAttribute(gE, expandOp)).toList)

          /*
           * For each AggregateExpression map the AttributeReference that it is
           * aggregating on to the position within the Expand project List.
           */
          val oaggExprIdToPos : Option[List[(ExprId, Int)]] =
            Utils.sequence(Utils.filterSomes(
            aggExpressions(aEs).map(
              exprIdToAttribute(_, expandOp)
            ).toList)
            )

          debugTranslation(
            s"""
               |  GroupExpression Mapping to Expand Projection:
               |    ${oattrRefIdxList}
               |  Aggregate Expressions Mapping to Expand Projection:
               |    ${oaggExprIdToPos}
             """.stripMargin)

          for(
            attrRefIdxList <- oattrRefIdxList;
            aggExprIdToPos <- oaggExprIdToPos
          ) yield {
            val geToPosMap = attrRefIdxList.toMap
            val aggExprIdToPosMap = aggExprIdToPos.toMap

            val grpSetInfos = projections.map { p =>

              debugTranslation(
                s"""
                  | Translating Expand Projection:
                  |   ${p}
                """.stripMargin

              )

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
              val expandOpGExps : ArrayBuffer[Expression] = ArrayBuffer()

              gEsAndaEs.foreach { t =>
                val gE: Expression = t._1
                val aE: Expression = t._2
                val gEAttrRef = geToPosMap(gE)._1
                val gExprDT = gEAttrRef.dataType
                val idx = geToPosMap(gE)._2
                val grpingExpr = p(idx)
                expandOpGExps += grpingExpr

                val transformedGExpr: Expression = aE match {
                  case ar@AttributeReference(nm, _, _, _) => Alias(grpingExpr, nm)(ar.exprId)
                  case al@Alias(_, nm) => Alias(grpingExpr, nm)(al.exprId)
                  case _ => gE.transformUp {
                    case x if x == gEAttrRef => grpingExpr
                  }
                }

                (aE, grpingExpr) match {
                  case (AttributeReference(
                  GroupingColumnName(_), IntegerType, _, _), _) =>
                    literlVals += ((aE, transformedGExpr))
                  case (Alias(AttributeReference(
                  GroupingColumnName(_), IntegerType, _, _), _), _) =>
                    literlVals += ((aE, transformedGExpr))
                  case (_, Literal(null, gExprDT)) => literlVals += ((aE, transformedGExpr))
                  case _ => ()
                }

              }

              /*
               * if this an expansion for CountDistinct then the aEs are not pushed to Druid.
               * But add the null values in the projection above the DruidRelation.
               */
              val nc = new NullCheckAggregateExpression(p, aggExprIdToPosMap)
              for {
                aE <- aEs
                tE <- nc.nullTransform(aE)
              } yield {
                literlVals += ((aE, tE))
                ()
              }

              val gI =
                GroupingInfo(gEs, expandOpGExps.toSeq, aEs, p, aggExprIdToPosMap, literlVals.toMap)

              debugTranslation(
                s"""
                   | GroupingInfo:
                   |   ${gI}
                """.stripMargin

              )

              gI
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
          GroupingInfo(gEs, gEs, aEs, Seq(), Map()))
      }
    }
    case _ => Seq()
  }

  def aggregateExpression(dqb: DruidQueryBuilder, aggExp: AggregateExpression)(
    implicit expandOpProjection : Seq[Expression],
    aEExprIdToPos : Map[ExprId, Int],
    aEToLiteralExpr: Map[Expression, Expression]) :
  Option[DruidQueryBuilder] = (aggExp, aggExp.aggregateFunction) match {
    case (_, Count(Literal(1, IntegerType) :: Nil)) => {
      val a = dqb.nextAlias
      Some(dqb.aggregate(FunctionAggregationSpec("count", a, "count")).
        outputAttribute(a, aggExp, aggExp.dataType, LongType))
    }
    case (_, Count(AttributeReference("1", _, _, _) :: Nil)) => {
      val a = dqb.nextAlias
      Some(dqb.aggregate(FunctionAggregationSpec("count", a, "count")).
        outputAttribute(a, aggExp, aggExp.dataType, LongType))
    }

    // TODO:
    // Instead of JS rewriting AVG as Sum, Cnt, Sum/Cnt
    // the expression should be rewritten generically. Introduce
    // a project on top with expr sum/cnt and push agg Sum, Count below.
    // This would avoid specialized average handling in JS and non JS paths.
    // This also neeeds to keep track of original aggregate and remove synthetic vc if
    // query didn't get pushed down.
    case (_, c) if JSAggGenerator.jSAvgCandidate(dqb, c) => {
      val sumAgg = new Sum(c.children.head)
      val countAgg = new Count(c.children)
      for (jsSumInf <- JSAggGenerator.jsAgg(dqb, new AggregateExpression(sumAgg, Partial, false),
        sumAgg, sqlContext.getConf(DruidPlanner.TZ_ID).toString);
           jsCountInf <- JSAggGenerator.jsAgg(jsSumInf._1,
             new AggregateExpression(countAgg, Partial, false),
             countAgg, sqlContext.getConf(DruidPlanner.TZ_ID).toString)) yield
        jsCountInf._1.avgExpression(aggExp, jsSumInf._2, jsCountInf._2)
    }

    // TODO: Make this the last option
    case (_, c) if JSAggGenerator.jSAggCandidate(dqb, c) => {
      for (jsADQB <- JSAggGenerator.jsAgg(dqb, aggExp, c,
        sqlContext.getConf(DruidPlanner.TZ_ID).toString)) yield
        jsADQB._1
    }
    case (_, c) => {
      val a = dqb.nextAlias
      (dqb, c, expandOpProjection, aEExprIdToPos) match {
        case CountDistinctAggregate(dN) =>
          Some(dqb.aggregate(new CardinalityAggregationSpec(a, List(dN))).
            outputAttribute(a, aggExp, aggExp.dataType, DoubleType))
        case SumMinMaxAvgAggregate(t) => t._1 match {
          case "avg" => {
            val dC: DruidColumn = t._2
            val aggFunc = dC.dataType match {
              case DruidDataType.Long => "longSum"
              case _ => "doubleSum"
            }
            val aggFuncDType = DruidDataType.sparkDataType(dC.dataType)
            val sumAlias = dqb.nextAlias
            val countAlias = dqb.nextAlias

            Some(
              dqb.aggregate(FunctionAggregationSpec(aggFunc, sumAlias, dC.name)).
                outputAttribute(sumAlias, null, aggFuncDType, aggFuncDType).
                aggregate(FunctionAggregationSpec("count", countAlias, "count")).
                outputAttribute(countAlias, null, LongType, LongType).
                avgExpression(aggExp, sumAlias, countAlias)
            )
          }
          case _ => {
            val dC: DruidColumn = t._2
            Some(dqb.aggregate(FunctionAggregationSpec(t._1, a, dC.name)).
              outputAttribute(a, aggExp, aggExp.dataType, DruidDataType.sparkDataType(dC.dataType))
            )
          }
        }
        case _ => None
      }
    }
  }

  private def attributeRef(arg: Expression)(
    implicit expandOpProjection : Seq[Expression], aEExprIdToPos : Map[ExprId, Int]):
  Option[String] = arg match {
    case ar : AttributeReference
      if aEExprIdToPos.contains(ar.exprId) && expandOpProjection(aEExprIdToPos(ar.exprId)) != ar =>
      attributeRef(expandOpProjection(aEExprIdToPos(ar.exprId)))
    case Cast(ar@AttributeReference(_, _, _, _), _)
      if aEExprIdToPos.contains(ar.exprId) && expandOpProjection(aEExprIdToPos(ar.exprId)) != ar =>
      attributeRef(expandOpProjection(aEExprIdToPos(ar.exprId)))
    case AttributeReference(name, _, _, _) => Some(name)
    case Cast(AttributeReference(name, _, _, _), _) => Some(name)
    case _ => None
  }

  private object CountDistinctAggregate {
    def unapply(t: (DruidQueryBuilder, AggregateFunction, Seq[Expression], Map[ExprId, Int])):
    Option[(String)]
    = {
      val dqb = t._1
      val aggFunc = t._2
      implicit val expandOpProjection = t._3
      implicit val aEExprIdToPos = t._4

      val r = for (c <- aggFunc.children.headOption if (aggFunc.children.size == 1);
                   dNm <- attributeRef(c);
                   dD <-
                   dqb.druidColumn(dNm) if dD.isInstanceOf[DruidDimension]
      ) yield (aggFunc, dD)

      r flatMap {
        case (p: HyperLogLogPlusPlus, dD) if dqb.drInfo.options.pushHLLTODruid => Some(dD.name)
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

    def unapply(t: (DruidQueryBuilder, AggregateFunction, Seq[Expression], Map[ExprId, Int])):
    Option[(String, DruidColumn)]
    = {
      val dqb = t._1
      val aggFunc = t._2
      implicit val expandOpProjection = t._3
      implicit val aEExprIdToPos = t._4

      val r = for (c <- aggFunc.children.headOption if (aggFunc.children.size == 1);
                   mNm <- attributeRef(c);
                   dM <- dqb.druidColumn(mNm) if dM.isInstanceOf[DruidMetric];
                   mDT <- Some(DruidDataType.sparkDataType(dM.dataType));
                   commonType <- HiveTypeCoercion.findTightestCommonTypeOfTwo(
                     aggFunc.dataType, mDT) if (commonType == mDT || aggFunc.isInstanceOf[Average])
      ) yield (aggFunc, commonType, dM)

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

  private object GroupingColumnName {
    def unapply(colName : String) : Option[String] = colName match {
      case VirtualColumn.groupingIdName => Some(colName)
      case "gid" => Some(colName)
      case _ => None
    }
  }

  private object AggregateMatch {
    def unapply(plan: LogicalPlan) :
    Option[(Aggregate,
      Seq[Expression], Seq[NamedExpression],
      Seq[NamedExpression],
      Expand, Seq[Seq[Expression]], LogicalPlan )] =
      plan match {
        case agg@Aggregate(gEs, aEs,
        expandOp@Expand(projections, _, child)) =>
          Some((agg, gEs, aEs, Seq(), expandOp, projections, child))
        case agg@Aggregate(gEs, aEs,
        Project(projectList, expandOp@Expand(projections, _, child))) =>
          Some((agg, gEs, aEs, projectList, expandOp, projections, child))
      case _ => None
    }
  }

  class NullCheckAggregateExpression(val expandOpProjection: Seq[Expression],
                                     val aEExprIdToPos: Map[ExprId, Int]) {

    private def isNull(ar : AttributeReference) =
      expandOpProjection(aEExprIdToPos(ar.exprId)) match {
      case Literal(null, _) => true
      case _ => false
    }

    private def isNull(aggFunc : AggregateFunction) : Boolean = {
      if (aggFunc.children.size == 1) {
        val c = aggFunc.children(0)
        c match {
          case ar : AttributeReference
            if aEExprIdToPos.contains(ar.exprId) => isNull(ar)
          case Cast(ar@AttributeReference(_, _, _, _), _)
            if aEExprIdToPos.contains(ar.exprId) => isNull(ar)
          case _ => false
        }
      } else {
        false
      }
    }

    private def isNull(e : Expression) : Boolean = {
      e.collect( { case a: AggregateExpression => a }).exists(a => isNull(a.aggregateFunction))
    }

    def nullTransform(e : Expression) : Option[Expression] = e match {
      case al@Alias(_, nm) if  (isNull(al))=>
        Some(Alias(Literal.create(null, e.dataType), nm)(al.exprId))
      case _ => None
    }

  }

}


