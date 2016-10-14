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

package org.sparklinedata.druid.query

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.druid.DruidQueryCostModel
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.{DruidDataType, DruidRelationInfo}
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ArrayBuffer


abstract class Transform extends Logging {

  val transformName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(sqlContext : SQLContext,
            drInfo : DruidRelationInfo,
            qSpec: QuerySpec): QuerySpec
}

/**
  * Based on [[org.apache.spark.sql.catalyst.rules.RuleExecutor]]
  */
abstract class TransformExecutor extends Logging {

  abstract class Strategy { def maxIterations: Int }

  case object Once extends Strategy { val maxIterations = 1 }

  case class FixedPoint(maxIterations: Int) extends Strategy

  protected case class Batch(name: String, strategy: Strategy, transforms: Transform*)

  protected val batches: Seq[Batch]

  def transform(sqlContext : SQLContext,
                drInfo : DruidRelationInfo, qSpec: QuerySpec): QuerySpec = {
    var curQSpec = qSpec

    batches.foreach { batch =>
      val batchStartQSpec = curQSpec
      var iteration = 1
      var lastQSpec = curQSpec
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curQSpec = batch.transforms.foldLeft(curQSpec) {
          case (qSpec, transform) =>
            transform(sqlContext,drInfo, qSpec)
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curQSpec == lastQSpec) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastQSpec = curQSpec
      }

      if (batchStartQSpec != curQSpec) {
        logDebug(
          s"""
             |=== Result of Batch ${batch.name} ===
             |$curQSpec
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curQSpec
  }
}

object AddCountAggregateForNoMetricsGroupByQuerySpec extends Transform {

  override def apply(sqlContext : SQLContext,
                     drInfo : DruidRelationInfo, qSpec: QuerySpec): QuerySpec =
    qSpec match {

    case gbSpec : GroupByQuerySpec if gbSpec.aggregations.isEmpty =>
      gbSpec.copy(aggregations =
      List(FunctionAggregationSpec("count", "addCountAggForNoMetricQuery", "count"))
      )
    case _ => qSpec
  }

}

object AllGroupingGroupByQuerySpecToTimeSeriesSpec extends Transform {

  override def apply(sqlContext : SQLContext,
                     drInfo : DruidRelationInfo, qSpec: QuerySpec): QuerySpec =
    qSpec match {

      case gbSpec : GroupByQuerySpec
        if (gbSpec.dimensionSpecs.isEmpty &&
          !gbSpec.having.isDefined &&
          !gbSpec.limitSpec.isDefined
          ) =>
        new TimeSeriesQuerySpec(
          gbSpec.dataSource,
          gbSpec.intervals,
          Left("all"),
          gbSpec.filter,
          gbSpec.aggregations,
          gbSpec.postAggregations,
          gbSpec.context
        )
      case _ => qSpec
    }

}

object BetweenFilterSpec extends Transform {

  def mergeIntoBetween(fSpec: FilterSpec): FilterSpec = fSpec match {
    case LogicalFilterSpec("and",
    List(
    BoundFilterSpec(_, dim1, Some(lower), lowerStrict, None, _, alphaNumeric1),
    BoundFilterSpec(_, dim2, None, None, Some(upper), upperStrict, alphaNumeric2)
    )
    ) if dim1 == dim2 => {
      new BoundFilterSpec(dim1, Some(lower), lowerStrict, Some(upper), upperStrict, alphaNumeric1)
    }
    case LogicalFilterSpec("and",
    List(
    BoundFilterSpec(_, dim2, None, None, Some(upper), upperStrict, alphaNumeric2),
    BoundFilterSpec(_, dim1, Some(lower), lowerStrict, None, _, alphaNumeric1)
    )
    ) if dim1 == dim2 => {
      new BoundFilterSpec(dim1, Some(lower), lowerStrict, Some(upper), upperStrict, alphaNumeric1)
    }
    case LogicalFilterSpec(typ, fields) => LogicalFilterSpec(typ, fields.map(mergeIntoBetween))
    case NotFilterSpec(typ, field) => NotFilterSpec(typ, mergeIntoBetween(field))
    case _ => fSpec
  }

  override def apply(sqlContext : SQLContext,
                     drInfo: DruidRelationInfo, qSpec: QuerySpec): QuerySpec = {
    if (!qSpec.filter.isDefined) {
      qSpec
    } else {
      val fSpec = qSpec.filter.get
      qSpec.setFilter(mergeIntoBetween(fSpec))
    }
  }

}

object CombineSpatialFilters extends Transform {

  protected def reduceSpatialConjuncts(logFilter : LogicalFilterSpec) : LogicalFilterSpec = {
    assert(logFilter.`type` == "and")
    val lF = logFilter.flatten
    val spatialFilters: MMap[String, SpatialFilterSpec] = MMap()
    val remainingFilters = ArrayBuffer[FilterSpec]()

    lF.fields.foreach { f =>
      f match {
        case sf@SpatialFilterSpec(d, _, _) => {
          if (!spatialFilters.contains(d)) {
            spatialFilters(d) = sf
          } else {
            spatialFilters(d) = spatialFilters(d).combine(sf)
          }
        }
        case _ => remainingFilters += f
      }
    }

    LogicalFilterSpec("and",
      (spatialFilters.values ++ remainingFilters).toList
    )
  }

  def reduceSpatialFilters(fSpec: FilterSpec): FilterSpec = fSpec match {
    case af@LogicalFilterSpec("and", _) => reduceSpatialConjuncts(af)
    case LogicalFilterSpec(typ, fields) => LogicalFilterSpec(typ, fields.map(reduceSpatialFilters))
    case NotFilterSpec(typ, field) => NotFilterSpec(typ, reduceSpatialFilters(field))
    case _ => fSpec
  }

  override def apply(sqlContext : SQLContext,
                     drInfo: DruidRelationInfo, qSpec: QuerySpec): QuerySpec = {
    if (!qSpec.filter.isDefined) {
      qSpec
    } else {
      val fSpec = qSpec.filter.get
      qSpec.setFilter(reduceSpatialFilters(fSpec))
    }
  }

}

object SearchQuerySpecTransform extends Transform {

  override def apply(sqlContext : SQLContext,
                     drInfo: DruidRelationInfo, qSpec: QuerySpec): QuerySpec = qSpec match {
    case GroupByQuerySpec(_, ds,
    List(DefaultDimensionSpec(_, dName, oName)),
    None,
    None,
    granularity,
    filter,
    List(),
    None,
    intervals,
    context
    ) if dName == oName &&
      QueryIntervals.queryForEntireDataSourceInterval(drInfo, qSpec) =>
      new SearchQuerySpec(
        ds,
        intervals,
        granularity,
        filter,
        List(dName),
        new InsensitiveContainsSearchQuerySpec(),
        Integer.MAX_VALUE,
        None,
        context
      )
    case GroupByQuerySpec(_, ds,
    List(DefaultDimensionSpec(_, dName, oName)),
    Some(LimitSpec(_, limValue, List(OrderByColumnSpec(ordName, "ascending")))),
    None,
    granularity,
    filter,
    List(),
    None,
    intervals,
    context
    ) if dName == oName && dName == ordName &&
      QueryIntervals.queryForEntireDataSourceInterval(drInfo, qSpec) =>
      new SearchQuerySpec(
        ds,
        intervals,
        granularity,
        filter,
        List(dName),
        new InsensitiveContainsSearchQuerySpec(),
        limValue,
        Some(SortSearchQuerySpec("lexicographic")),
        context
      )
    case _ => qSpec
  }
}

object TopNQueryTransform extends Transform {

  def topNMetric(drInfo: DruidRelationInfo,
                 ordColumn : String,
                 ordDirection : String) : TopNMetricSpec = {

    val dT = drInfo.druidDS.metric(ordColumn).map(_.dataType).getOrElse(DruidDataType.Long)

    (dT, ordDirection.toLowerCase()) match {
      case (DruidDataType.Long | DruidDataType.Float, "descending") =>
        new NumericTopNMetricSpec(ordColumn)
      case (DruidDataType.Long | DruidDataType.Float, "ascending") =>
        new InvertedTopNMetricSpec(new NumericTopNMetricSpec(ordColumn))
      case (_, "descending") =>
        new LexiographicTopNMetricSpec(ordColumn)
      case (_, "ascending") =>
        new InvertedTopNMetricSpec(new LexiographicTopNMetricSpec(ordColumn))
      case _ => new LexiographicTopNMetricSpec(ordColumn)
    }
  }

  override def apply(sqlContext : SQLContext,
                     drInfo: DruidRelationInfo, qSpec: QuerySpec): QuerySpec = qSpec match {
    case GroupByQuerySpec(_, ds,
    List(dimSpec@DefaultDimensionSpec(_, dName, oName)),
    Some(LimitSpec(_, limValue, List(OrderByColumnSpec(ordName, ordDirection)))),
    None,
    granularity,
    filter,
    aggregations,
    postAggregations,
    intervals,
    context
    ) if drInfo.options.allowTopN(sqlContext) &&
      limValue < drInfo.options.topNMaxThreshold(sqlContext) &&
      oName != ordName => {
      var c = context.get
      c = c.copy(minTopNThreshold = Some(limValue))
      new TopNQuerySpec(
        ds,
        intervals,
        granularity,
        filter,
        aggregations,
        postAggregations,
        dimSpec,
        limValue,
        topNMetric(drInfo, ordName, ordDirection),
        Some(c)
      )
    }
    case _ => qSpec
  }
}

object QuerySpecTransforms extends TransformExecutor {

  override protected val batches: Seq[Batch] = Seq(
    Batch("dimensionQueries", FixedPoint(100),
      SearchQuerySpecTransform, AddCountAggregateForNoMetricsGroupByQuerySpec, BetweenFilterSpec),
    Batch("combineSpatialFilters", Once, CombineSpatialFilters),
    Batch("timeseries", Once, AllGroupingGroupByQuerySpecToTimeSeriesSpec),
    Batch("topN", Once, TopNQueryTransform)
  )

}
