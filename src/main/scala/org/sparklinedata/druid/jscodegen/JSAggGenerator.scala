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

package org.sparklinedata.druid.jscodegen

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, LeafExpression}
import org.apache.spark.sql.types._
import org.sparklinedata.druid.{JavascriptAggregationSpec, DruidQueryBuilder}
import org.sparklinedata.druid.metadata.DruidTimeDimension

import scala.language.reflectiveCalls

// TODO: Handle NULL in dimension (non Time Dim)
// 1. If there is at least one non null value then Sum/Min/Max/Count would work
// but would fail otherwise. Needs to handle the case where every value is NULL.
// 2. When every value is null for a dim, how do we encode null in the value to spark.
case class JSAggGenerator(dqb: DruidQueryBuilder, agg: AggregateFunction,
                          tz_id: String) extends Logging {

  private[this] def getAgg(arg: String): Option[String] = agg match {
    case Sum(e) => Some(s"""current + $arg""".stripMargin)
//    case Min(e) => Some(s"""(($arg) ? Math.min(current, $arg) : current)""".stripMargin)
    case Min(e) => Some(s"""Math.min(current, $arg)""".stripMargin)
    case Max(e) => Some(s"""Math.max(current, $arg)""".stripMargin)
    case Count(e) => Some(s"""(($arg) ? (current + 1) : current)""".stripMargin)
    case _ => None
  }

  private[this] def getCombine(partialA: String, partialB: String): Option[String] = agg match {
    case Sum(e) => Some(s"""($partialA + $partialB)""".stripMargin)
    case Min(e) => Some(s"""Math.min($partialA, $partialB)""".stripMargin)
    case Max(e) => Some(s"""Math.max($partialA, $partialB)""".stripMargin)
    case Count(e) => Some(s"""($partialA + $partialB)""".stripMargin)
    case _ => None
  }

  private[this] def getReset: Option[String] = agg match {
    case Sum(e) => Some(s"""0""".stripMargin)
    case Min(e) => Some(s"""Number.POSITIVE_INFINITY""".stripMargin)
    case Max(e) => Some(s"""Number.NEGATIVE_INFINITY""".stripMargin)
    case Count(e) => Some(s"""0""".stripMargin)
    case _ => None
  }

  private[this] val jsAggType: Option[DataType] =
    agg.dataType match {
      case ShortType | IntegerType | LongType |
           FloatType | DoubleType => Some(DoubleType)
      case TimestampType => Some(LongType)
      case _ => None
    }

  val druidType: Option[DataType] =
    agg match {
      case Count(_) => Some(LongType)
      case _ =>
        agg.dataType match {
          case ShortType | IntegerType | LongType |
               FloatType | DoubleType => Some(DoubleType)
          case TimestampType => Some(TimestampType)
          case _ => None
        }
    }

  private[this] val jsc: Option[(JSCodeGenerator, String)] =
    for (c <- agg.children.headOption
         if (agg.children.size == 1 && !agg.isInstanceOf[Average]);
         (sc, tf) <- JSAggGenerator.simplifyExpr(dqb, c, tz_id);
         commonType <- jsAggType) yield
      (JSCodeGenerator(dqb, sc, true, true, tz_id, commonType), tf)

  val fnAggregate: Option[String] =
    for (cg <- jsc; fne <- cg._1.fnElements; ret <- getAgg(fne._2)) yield
      s"""function (${("current" :: cg._1.fnParams).mkString(", ")}) {${fne._1} return ${ret};}
    """.stripMargin

  val fnCombine: Option[String] =
    for (cg <- jsc; fne <- cg._1.fnElements; ret <- getCombine("partialA", "partialB")) yield
      s"""function (partialA, partialB) {return ${ret};}""".stripMargin

  val fnReset: Option[String] =
    for (cg <- jsc; fne <- cg._1.fnElements; ret <- getReset) yield
      s"""function() { return $ret; }"""

  val aggFnName: Option[String] = agg match {
    case Sum(e) => Some("SUM")
    case Min(e) => Some("MIN")
    case Max(e) => Some("MAX")
    case Count(e) => Some("COUNT")
    case _ => None
  }

  val fnParams: Option[List[String]] = for (cg <- jsc) yield cg._1.fnParams

  val valTransFormFn = if (!jsc.isEmpty) jsc.get._2 else null
}

object JSAggGenerator {
  def jSAvgCandidate(dqb: DruidQueryBuilder, af: AggregateFunction):
  Boolean = af match {
    case Average(_)
      if ((af.children.size == 1) && (!af.children.head.isInstanceOf[LeafExpression] ||
        af.children.head.references.map(_.name).toSet.size > 1 ||
        (af.children.head.references.map(_.name).toSet &
          dqb.drInfo.dimensionNamesSet).size > 0)) => true
    case _ => false
  }

  def jSAggCandidate(dqb: DruidQueryBuilder, af: AggregateFunction):
  Boolean = af match {
    case Sum(_) | Min(_) | Max(_) | Average(_) | Count(_)
      if ((af.children.size == 1) && (!af.children.head.isInstanceOf[LeafExpression] ||
        af.children.head.references.map(_.name).toSet.size > 1 ||
        (af.children.head.references.map(_.name).toSet &
          dqb.drInfo.dimensionNamesSet).size > 0)) => true
    case _ => false
  }

  def simplifyExpr(dqb: DruidQueryBuilder, e: Expression, tz_id: String):
  Option[(Expression, String)] = {
    e match {
      case Cast(a@AttributeReference(nm, _, _, _), TimestampType)
        if dqb.druidColumn(nm).nonEmpty &&
          dqb.druidColumn(nm).get.isInstanceOf[DruidTimeDimension] =>
          Some((Cast(a, LongType), "toTSWithTZAdj"))
      case _ => Some(e, null)
    }
  }

  def jsAgg(dqb: DruidQueryBuilder, aggExp: Expression,
            c: AggregateFunction, tz: String): Option[(DruidQueryBuilder, String)] = {
    val jsag = JSAggGenerator(dqb, c, tz);
    for (fna <- jsag.fnAggregate; fnc <- jsag.fnCombine; fnr <- jsag.fnReset;
         aggFnName <- jsag.aggFnName; fnName = dqb.nextAlias(aggFnName);
         fnparams <- jsag.fnParams; at <- jsag.druidType) yield {
      (dqb.aggregate(JavascriptAggregationSpec("javascript", fnName,
        fnparams, fna, fnc, fnr)).
        outputAttribute(fnName, aggExp, aggExp.dataType, at, jsag.valTransFormFn), fnName)
    }
  }
}