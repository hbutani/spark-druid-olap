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

package org.sparklinedata.druid

import org.apache.spark.Logging
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{Extraction, ShortTypeHints}
import org.sparklinedata.druid.client.QueryResultRowSerializer
import org.sparklinedata.druid.metadata.{EqualityCondition, FunctionalDependencyType}
import org.sparklinedata.druid.metadata.{StarRelationInfo, StarSchemaInfo}

import scala.util.Random

object Utils extends Logging {

  implicit val jsonFormat = Serialization.formats(
    ShortTypeHints(
      List(
        classOf[AlphaNumericTopNMetricSpec],
        classOf[ArithmeticPostAggregationSpec],
        classOf[CardinalityAggregationSpec],
        classOf[ComparisonHavingSpec],
        classOf[ConstantPostAggregationSpec],
        classOf[DefaultDimensionSpec],
        classOf[DurationGranularitySpec],
        classOf[ExtractionDimensionSpec],
        classOf[ExtractionFunctionSpec],
        classOf[FieldAccessPostAggregationSpec],
        classOf[FilteredAggregationSpec],
        classOf[FunctionAggregationSpec],
        classOf[GroupByQuerySpec],
        classOf[HavingSpec],
        classOf[HyperUniqueCardinalityPostAggregationSpec],
        classOf[InvertedTopNMetricSpec],
        classOf[JavascriptAggregationSpec],
        classOf[JavaScriptExtractionFunctionSpec],
        classOf[JavascriptFilterSpec],
        classOf[JavascriptPostAggregationSpec],
        classOf[LexiographicTopNMetricSpec],
        classOf[LimitSpec],
        classOf[LogicalBinaryOpHavingSpec],
        classOf[LogicalFilterSpec],
        classOf[NotFilterSpec],
        classOf[NotOpHavingSpec],
        classOf[NumericTopNMetricSpec],
        classOf[OrderByColumnSpec],
        classOf[PartialExtractionFunctionSpec],
        classOf[PeriodGranularitySpec],
        classOf[RegexExtractionFunctionSpec],
        classOf[RegexFilterSpec],
        classOf[SearchQueryExtractionFunctionSpec],
        classOf[SelectorFilterSpec],
        classOf[TimeFormatExtractionFunctionSpec],
        classOf[TimeParsingExtractionFunctionSpec],
        classOf[TimeSeriesQuerySpec],
        classOf[TopNMetricSpec],
        classOf[TopNQuerySpec],
        classOf[StarSchemaInfo],
        classOf[StarRelationInfo],
        classOf[EqualityCondition],
        classOf[LookUpMap],
        classOf[InExtractionFnSpec],
        classOf[ExtractionFilterSpec]
      )
    )
  ) +
    new EnumNameSerializer(FunctionalDependencyType) + new QueryResultRowSerializer ++
    org.json4s.ext.JodaTimeSerializers.all

  def logQuery(dq: DruidQuery): Unit = {
    log.info("\nDruid Query:\n" + pretty(render(Extraction.decompose(dq))))
  }

  def logQuery(qSpec: QuerySpec): Unit = {
    log.info("\nDruid Query:\n" + pretty(render(Extraction.decompose(qSpec))))
  }

  def logStarSchema(ss: StarSchemaInfo): Unit = {
    log.info("\nStar Schema:\n" + pretty(render(Extraction.decompose(ss))))
  }

  def queryToString(dq: DruidQuery): String = pretty(render(Extraction.decompose(dq)))

  def queryToString(qSpec: QuerySpec): String = pretty(render(Extraction.decompose(qSpec)))

  /**
    * from fpinscala book
    *
    * @param a
    * @tparam A
    * @return
    */
  def sequence[A](a: List[Option[A]]): Option[List[A]] =
    a match {
      case Nil => Some(Nil)
      case h :: t => h flatMap (hh => sequence(t) map (hh :: _))
    }

  def filterSomes[A](a : List[Option[A]]) : List[Option[A]] =
  a.filter{ case Some(x) => true; case _ => false }

  def permute(s: String): String = StringPermute.permute(s)
}

/**
  * The [[https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle][Fischer Yates Algortihm]]
  */
private[sparklinedata] object StringPermute {

  val r = new Random()

  def permute(s: String): String = {

    val c = s.toArray
    val sz = c.length

    for (i <- sz - 1 to 1 by -1) {
      val j = r.nextInt(i)
      val c1 = c(j)
      c(j) = c(i)
      c(i) = c1
    }

    c.mkString("")
  }

}
