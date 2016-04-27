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
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.sparklinedata.druid.DruidQueryBuilder

import scala.language.reflectiveCalls


case class JSAggGenerator(dqb: DruidQueryBuilder, agg: AggregateFunction,
                          tz_id: String) extends Logging {

  private[this] def getAgg(arg: String): Option[String] = agg match {
    case Sum(e) => Some(s"""current + $arg""".stripMargin)
    case Min(e) => Some(s"""Math.min(current, $arg)""".stripMargin)
    case Max(e) => Some(s"""Math.max(current, $arg)""".stripMargin)
    case _ => None
  }

  private[this] def getCombine(partialA: String, partialB: String): Option[String] = agg match {
    case Sum(e) => Some(s"""($partialA + $partialB)""".stripMargin)
    case Min(e) => Some(s"""Math.min($partialA, $partialB)""".stripMargin)
    case Max(e) => Some(s"""Math.max($partialA, $partialB)""".stripMargin)
    case _ => None
  }

  private[this] def getReset: Option[String] = agg match {
    case Sum(e) => Some(s"""0""".stripMargin)
    case Min(e) => Some(s"""Number.POSITIVE_INFINITY""".stripMargin)
    case Max(e) => Some(s"""Number.NEGATIVE_INFINITY""".stripMargin)
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
    agg.dataType match {
      case ShortType | IntegerType | LongType |
           FloatType | DoubleType => Some(DoubleType)
      case TimestampType => Some(TimestampType)
      case _ => None
    }

  private[this] val jsc: Option[JSCodeGenerator] =
    for (c <- agg.children.headOption
         if (agg.children.size == 1 && !agg.isInstanceOf[Average]);
         commonType <- jsAggType) yield
      JSCodeGenerator(dqb, c, true, true, tz_id, commonType)

  val fnAggregate: Option[String] =
    for (cg <- jsc; fne <- cg.fnElements; ret <- getAgg(fne._2)) yield
      s"""function (${("current" :: cg.fnParams).mkString(", ")}) {${fne._1} return ${ret};}
    """.stripMargin

  val fnCombine: Option[String] =
    for (cg <- jsc; fne <- cg.fnElements; ret <- getCombine("partialA", "partialB")) yield
      s"""function (partialA, partialB) {return ${ret};}""".stripMargin

  val fnReset: Option[String] =
    for (cg <- jsc; fne <- cg.fnElements; ret <- getReset) yield
      s"""function() { return $ret; }"""

  val aggFnName: Option[String] = agg match {
    case Sum(e) => Some("SUM")
    case Min(e) => Some("MIN")
    case Max(e) => Some("MAX")
    case _ => None
  }

  val fnParams: Option[List[String]] = for (cg <- jsc) yield cg.fnParams
}

object JSAggGenerator {
  def JSAggCandidate(dqb: DruidQueryBuilder, af: AggregateFunction):
  Boolean = af match {
    case Sum(_) | Min(_) | Max(_)
      if ((af.children.size == 1) && (!af.children.head.isInstanceOf[LeafExpression] ||
        af.children.head.references.map(_.name).toSet.size > 1 ||
        (af.children.head.references.map(_.name).toSet &
          dqb.drInfo.dimensionNamesSet).size > 0)) => true
    case _ => false
  }
}