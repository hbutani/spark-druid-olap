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

import java.io.InputStream
import java.util.Locale

import org.apache.spark.sql.sources.druid._

import scala.collection.breakOut
import org.apache.spark.sql.types._
import org.joda.time.Interval
import org.sparklinedata.druid.client._
import org.sparklinedata.druid.metadata.{DruidDataSource, DruidDataType, DruidRelationInfo, DruidSegmentInfo}

sealed trait ExtractionFunctionSpec {
  val `type`: String
}

/**
 * In SQL this is an 'like' or rlike' predicate on a column being grouped on.
  *
  * @param `type`
 * @param expr
 */
case class RegexExtractionFunctionSpec(val `type`: String, val expr: String)
  extends ExtractionFunctionSpec

/**
 * In SQL this is a grouping expression of the form 'if col rlike regex then regex else null'
  *
  * @param `type`
 * @param expr
 */
case class PartialExtractionFunctionSpec(val `type`: String, val expr: String)
  extends ExtractionFunctionSpec

/**
 * In SQL this is a contains predicate on a column being grouped on.
  *
  * @param `type`
 * @param query
 */
case class SearchQueryExtractionFunctionSpec(val `type`: String, val query: String)
  extends ExtractionFunctionSpec

/**
 * In SQL this is a withTimeZone and field extraction functions applied to the time dimension
 * columns. Assume time functions are expressed using
 * [[https://github.com/SparklineData/spark-datetime SparklineData-sparkdatetime package]].
  *
  * @param `type`
 * @param format
 * @param timeZone
 * @param locale
 */
case class TimeFormatExtractionFunctionSpec(val `type`: String,
                                            val format: String,
                                            val timeZone: Option[String],
                                            val locale: Option[String])
  extends ExtractionFunctionSpec {
  def this(format: String,
           timeZone: Option[String]) =
    this("timeFormat", format, timeZone, Some(Locale.getDefault.toString))
}

case class TimeParsingExtractionFunctionSpec(val `type`: String,
                                             val timeFormat: String,
                                             val resultFormat: String)
  extends ExtractionFunctionSpec {
  def this(timeFormat: String, resultFormat: String) =
    this("time", timeFormat, resultFormat)
}

case class JavaScriptExtractionFunctionSpec(val `type`: String,
                                            val `function`: String,
                                            val injective: Boolean = false)
  extends ExtractionFunctionSpec {
  def this(fn: String) = this("javascript", fn)
}

case class LookUpMap(val `type`: String, val `map`: Map[String, String])

case class InExtractionFnSpec(val `type`: String, lookup: LookUpMap)
  extends ExtractionFunctionSpec {
  def this(valLst: List[String]) = this("lookup",
    LookUpMap("map", valLst.map(x => (x, "true"))(breakOut)))
}

/**
  * As defined in [[http://druid.io/docs/latest/querying/dimensionspecs.html]]
 */
sealed trait DimensionSpec {
  val `type`: String
  val dimension: String
  val outputName: String

  def sparkDataType(dDS : DruidDataSource) : DataType = StringType
}

/**
 * In SQL these are columns being grouped on.
  *
  * @param `type`
 * @param dimension
 * @param outputName
 */
case class DefaultDimensionSpec(val `type`: String, val dimension: String,
                                val outputName: String) extends DimensionSpec {
  def this(dimension : String,
           outputName: String) = this("default", dimension, outputName)
  def this(dimension : String) = this("default", dimension, dimension)
}

case class ExtractionDimensionSpec(val `type`: String,
                                   val dimension: String,
                                   val outputName: String,
                                   extractionFn: ExtractionFunctionSpec) extends DimensionSpec {
  def this(dimension: String,
           outputName: String,
           extractionFn: ExtractionFunctionSpec) =
    this("extraction", dimension, outputName, extractionFn)
}

sealed trait GranularitySpec {
  val `type`: String
}

case class DurationGranularitySpec(`type`: String, duration: Long) extends GranularitySpec

case class PeriodGranularitySpec(`type`: String, period: String,
                                 timeZone: Option[String],
                                 origin: Option[String]) extends GranularitySpec {
  def this(period: String) = this("period", period, None, None)
}

sealed trait FilterSpec {
  val `type`: String
}

case class SelectorFilterSpec(`type`: String,
                              dimension: String,
                              value: String) extends FilterSpec {
  def this(dimension: String,
           value: String) = this("selector", dimension, value)
}

case class RegexFilterSpec(`type`: String,
                           dimension: String,
                           pattern: String) extends FilterSpec {
  def this(dimension: String,
           pattern: String) = this("regex", dimension, pattern)
}

case class StringContainsSpec(`type`: String,
                              dimension: String,
                              value: String) extends FilterSpec {
  def this(dimension: String,
           value: String) = this("contains", dimension, value)
}

case class LogicalFilterSpec(`type`: String,
                             fields: List[FilterSpec]) extends FilterSpec

case class NotFilterSpec(`type`: String,
                         field: FilterSpec) extends FilterSpec

case class ExtractionFilterSpec(`type`: String,
                                dimension: String,
                                value: String,
                                extractionFn: InExtractionFnSpec) extends FilterSpec {
  def this(dimension: String, valList: List[String]) = this("extraction", dimension, "true",
    new InExtractionFnSpec(valList))
}

/**
 * In SQL an invocation on a special JSPredicate is translated to this FilterSpec.
 * JSPredicate has the signature jspredicate(colum, jsFuncCodeAsString)
  *
  * @param `type`
 * @param dimension
 * @param function
 */
case class JavascriptFilterSpec(`type`: String,
                                dimension: String,
                                function: String) extends FilterSpec {
  def this(dimension: String,
           function: String) = this("javascript", dimension, function)

}

object JavascriptFilterSpec {

  def jsFn(compareOp : String, value : String) : String = {
     s"function(x) { return(x $compareOp '$value') }"
  }

  def create(dimension: String,
           compareOp : String,
           value : String) = new JavascriptFilterSpec(dimension, jsFn(compareOp, value))

}

case class BoundFilterSpec(`type`: String,
                           dimension: String,
                           lower: Option[String],
                           lowerStrict : Option[Boolean],
                           upper: Option[String],
                           upperStrict : Option[Boolean],
                           alphaNumeric: Boolean) extends FilterSpec {

  def this(dimension: String,
           lower: Option[String],
           lowerStrict : Option[Boolean],
           upper: Option[String],
           upperStrict : Option[Boolean],
           alphaNumeric: Boolean) =
    this("bound", dimension, lower, lowerStrict, upper, upperStrict, alphaNumeric)
}

case class RectangularBound(minCoords : Array[Double],
                            maxCoords : Array[Double],
                           `type` : String = "rectangular")

case class SpatialFilterSpec(
                            dimension : String,
                            bound : RectangularBound,
                            `type` : String = "spatial"
                            ) extends FilterSpec

sealed trait AggregationSpec {
  val `type`: String
  val name : String

  // TODO: get rid of this method: eventually translation of sql infers DataType
  def sparkDataType(dDS : DruidDataSource) : DataType
}

/**
 * In SQL an aggregation expression on a metric is translated to this Spec.
  *
  * @param `type` can be "count", "longSum", "doubleSum", "min", "max", "hyperUnique"
 * @param name
 * @param fieldName
 */
case class FunctionAggregationSpec(val `type`: String,
                                   val name: String,
                                   val fieldName: String
                                    ) extends AggregationSpec {
  def sparkDataType(dDS : DruidDataSource) : DataType =
    dDS.metric(fieldName).map(c =>
      DruidDataType.sparkDataType(c.dataType)).getOrElse(
        throw new DruidDataSourceException(s"Unknown field $fieldName"))
}

/**
 * In SQL a count(distinct dimColumn) is translated to this Spec.
  *
  * @param `type`
 * @param name
 * @param fieldNames
 * @param byRow
 */
case class CardinalityAggregationSpec(val `type`: String,
                                      val name: String,
                                      val fieldNames: List[String],
                                      val byRow: Boolean
                                       ) extends AggregationSpec {
  def this(name: String,
           fieldNames: List[String]) = this("cardinality", name, fieldNames, true)

  def sparkDataType(dDS : DruidDataSource) : DataType = DoubleType
}

case class HyperUniqueAggregationSpec(val `type`: String,
                                      val name: String,
                                      val fieldName : String) extends AggregationSpec {

  def this(name: String,
           fieldName: String) = this("hyperUnique", name, fieldName)

  def sparkDataType(dDS : DruidDataSource) : DataType = DoubleType

}

/**
 * In SQL this is an invocation on a special JSAgg function. Its signature is
 * JSAggLong(metricCol, aggFnCode, combineFnCode, resetFnCode). Similar kind of
 * function double metrics: JSAggDouble.
  *
  * @param `type`
 * @param name
 * @param fieldNames
 * @param fnAggregate
 * @param fnCombine
 * @param fnReset
 */
case class JavascriptAggregationSpec(val `type`: String,
                                     val name: String,
                                     val fieldNames: List[String],
                                     val fnAggregate: String,
                                     val fnCombine: String,
                                     val fnReset: String
                                      ) extends AggregationSpec {

  // for now assuming it is always Double
  def sparkDataType(dDS : DruidDataSource) : DataType = DoubleType
}

/**
 * In SQL this is an aggregation on an If condition. For example
 * sum(if dimCol = value then metric else null end)
  *
  * @param `type`
 * @param filter
 * @param aggregator
 */
case class FilteredAggregationSpec(val `type`: String,
                                   val name: String,
                                   val filter: SelectorFilterSpec,
                                   val aggregator: AggregationSpec
                                    ) extends AggregationSpec {

  def sparkDataType(dDS : DruidDataSource) : DataType = aggregator.sparkDataType(dDS)
}

sealed trait PostAggregationSpec {
  val `type`: String
  val name: String

  // TODO: get rid of this method: eventually translation of sql infers DataType
  // for now set them to DoubleType
  def sparkDataType(dDS : DruidDataSource) : DataType = DoubleType
}

case class FieldAccessPostAggregationSpec(val `type`: String,
                                          val fieldName: String
                                           ) extends PostAggregationSpec {
  def this(fieldName: String) = this("fieldAccess", fieldName)
  override val name = fieldName
}

case class ConstantPostAggregationSpec(val `type`: String,
                                       val name: String,
                                       val value: Double
                                        ) extends PostAggregationSpec

case class HyperUniqueCardinalityPostAggregationSpec(val `type`: String,
                                                     val name: String,
                                                     val fieldName: String
                                                      ) extends PostAggregationSpec

/**
 * In SQL this is an expression involving at least 1 aggregation expression
  *
  * @param `type`
 * @param name
 * @param fn can be +, -, *, /
 * @param fields
 * @param ordering used if the ordering is on the post aggregation expression.
 */
case class ArithmeticPostAggregationSpec(val `type`: String,
                                         val name: String,
                                         val fn: String,
                                         val fields: List[PostAggregationSpec],
                                         val ordering: Option[String]
                                          ) extends PostAggregationSpec {
  def this(name: String,
  fn: String,
  fields: List[PostAggregationSpec],
  ordering: Option[String]) = this("arithmetic", name, fn, fields, ordering)
}

case class JavascriptPostAggregationSpec(val `type`: String,
                                         val name: String,
                                         val fields: List[PostAggregationSpec],
                                         val function: String
                                          ) extends PostAggregationSpec

/**
 *
 * @param dimension
 * @param direction can be "ascending"|"descending"
 */
case class OrderByColumnSpec(val dimension: String,
                             val direction: String) {

  def this(dimension: String, asc : Boolean) =
    this(dimension, if (asc) "ascending" else "descending")

  def this(dimension: String) =
    this(dimension, "ascending")
}

case class LimitSpec(val `type`: String,
                     val limit: Int,
                     val columns: List[OrderByColumnSpec]) {

  def this(limit: Int,
           columns: List[OrderByColumnSpec]) = this("default", limit, columns)

  def this(limit: Int,
           columns: OrderByColumnSpec*) = this("default", limit, columns.toList)
}

sealed trait HavingSpec {
  val `type`: String
}

/**
 *
 * @param `type` is greaterThan, equalTo, lessThan
 * @param aggregation
 * @param value
 */
case class ComparisonHavingSpec(val `type`: String,
                                val aggregation: String,
                                val value: Double)

case class LogicalBinaryOpHavingSpec(val `type`: String,
                                     val havingSpecs: List[HavingSpec])

case class NotOpHavingSpec(val `type`: String,
                           val havingSpec: HavingSpec)

sealed trait TopNMetricSpec {
  val `type`: String
}

case class NumericTopNMetricSpec(
                                  val `type`: String,
                                  val metric: String
                                  ) extends TopNMetricSpec {
  def this(metric: String) = this("numeric", metric)
}

case class LexiographicTopNMetricSpec(
                                       val `type`: String,
                                       val previousStop: String
                                       ) extends TopNMetricSpec {
  def this(previousStop: String) = this("lexicographic", previousStop)
}

case class AlphaNumericTopNMetricSpec(
                                       val `type`: String,
                                       val previousStop: String
                                       ) extends TopNMetricSpec

case class InvertedTopNMetricSpec(
                                   val `type`: String,
                                   val metric: TopNMetricSpec
                                   ) extends TopNMetricSpec {
  def this(metric: TopNMetricSpec) = this("inverted", metric)
}


case class SegmentInterval(itvl : String,
                           ver : String,
                           part : Option[Int]
                          )

case class SegmentIntervals(`type` : String,
                            segments : List[SegmentInterval]
                           ) {
  def this(segInAssignments: List[(DruidSegmentInfo, Interval)]) = {
    this("segments", segInAssignments.map {
      case (segInfo, in) =>
        val itvl: String = in.toString
        val ver: String = segInfo.version
        val part: Option[Int] = segInfo.shardSpec.flatMap(_.partitionNum)
        SegmentInterval(itvl, ver, part)
    }
    )
  }
}

object SegmentIntervals {

  def segmentIntervals(segInAssignments: List[DruidSegmentInfo]) : SegmentIntervals = {
    SegmentIntervals("segments", segInAssignments.map {
      case segInfo =>
        val itvl: String = segInfo.interval
        val ver: String = segInfo.version
        val part: Option[Int] = segInfo.shardSpec.flatMap(_.partitionNum)
        SegmentInterval(itvl, ver, part)
    }
    )
  }
}

trait SearchQueryQuerySpec {
  self : Product =>
}

case class InsensitiveContainsSearchQuerySpec (
                                                `type` : String,
                                                value : String
                                              ) extends SearchQueryQuerySpec {
  def this() = this("insensitive_contains", "")
}

case class SortSearchQuerySpec(
                                `type` : String
                              )

case class QuerySpecContext(
                             var queryId : String,
                           timeout : Option[Long] = None,
                           priority : Option[Int] = None,
                             useCache : Option[Boolean] = None,
                             populateCache : Option[Boolean] = None,
                             bySegment : Option[Boolean] = None,
                             chunkPeriod : Option[String] = None,
                             minTopNThreshold : Option[Int] = None,
                             maxResults : Option[Int] = None,
                             maxIntermediateRows : Option[Int] = None,
                             groupByIsSingleThreaded : Option[Boolean] = None,
                             var groupByStrategy : Option[String] = None
                           )

sealed trait QuerySpec {
  self : Product =>
  val queryType: String
  val dataSource: String

  def intervalList: List[String]
  def setIntervals(ins : List[Interval]) : QuerySpec
  def setSegIntervals(segIns : List[(DruidSegmentInfo, Interval)]) : QuerySpec

  def dimensionSpecs : List[DimensionSpec] = Nil

  def filter : Option[FilterSpec]

  def setFilter(fSpec : FilterSpec) : QuerySpec

  def context : Option[QuerySpecContext]

  def apply(useSmile : Boolean,
            is : InputStream,
            druidQuerySvrConn: DruidClient,
            onDone : => Unit = (),
            fromList : Boolean = false) : CloseableIterator[ResultRow]

  def schemaFromQuerySpec(dInfo : DruidRelationInfo) : StructType

  def executeQuery(queryClient : DruidQueryServerClient) : CloseableIterator[ResultRow] =
    queryClient.executeQueryAsStream(this)

  def mapSparkColNameToDruidColName(druidRelationInfo: DruidRelationInfo) : Map[String, String] =
    Map()

}

abstract class AggQuerySpec extends QuerySpec {
  self : Product =>

  def dimensions : List[DimensionSpec] = Nil
  override def dimensionSpecs : List[DimensionSpec] = dimensions
  def aggregations : List[AggregationSpec] = Nil
  def postAggregations : Option[List[PostAggregationSpec]] = None

  def schemaFromQuerySpec(dInfo : DruidRelationInfo) : StructType = {
    val fields : List[StructField] = dimensionSpecs.map{ d =>
      new StructField(d.outputName, d.sparkDataType(dInfo.druidDS))
    } ++
      aggregations.map {a =>
        new StructField(a.name, a.sparkDataType(dInfo.druidDS))
      } ++
      postAggregations.map{ ps =>
        ps.map {p =>
          new StructField(p.name, p.sparkDataType(dInfo.druidDS))
        }
      }.getOrElse(Nil)
    StructType(fields)
  }

  override def apply(useSmile : Boolean,
                     is : InputStream,
                     druidQuerySvrConn: DruidClient,
                     onDone : => Unit = (),
                     fromList : Boolean = false) : CloseableIterator[ResultRow] =
    DruidQueryResultIterator(useSmile, is, onDone, fromList)

}

case class GroupByQuerySpec(
                             val queryType: String,
                             val dataSource: String,
                             override val dimensions: List[DimensionSpec],
                             val limitSpec: Option[LimitSpec],
                             val having: Option[HavingSpec],
                             val granularity: Either[String,GranularitySpec],
                             val filter: Option[FilterSpec],
                             override val aggregations: List[AggregationSpec],
                             override val postAggregations: Option[List[PostAggregationSpec]],
                             val intervals: List[String],
                             override val context : Option[QuerySpecContext]
                             ) extends AggQuerySpec {
  def this(dataSource: String,
           dimensions: List[DimensionSpec],
           limitSpec: Option[LimitSpec],
           having: Option[HavingSpec],
           granularity: Either[String,GranularitySpec],
           filter: Option[FilterSpec],
           aggregations: List[AggregationSpec],
           postAggregations: Option[List[PostAggregationSpec]],
           intervals: List[String],
           context : Option[QuerySpecContext]) = this("groupBy",
    dataSource, dimensions, limitSpec, having, granularity, filter,
    aggregations, postAggregations, intervals, context)

  def setIntervals(ins : List[Interval]) = this.copy(intervals = ins.map(_.toString))
  def intervalList: List[String] = intervals

  def setSegIntervals(segIns : List[(DruidSegmentInfo, Interval)]) : QuerySpec =
    GroupByQuerySpecWithSegIntervals(
      queryType,
      dataSource,
      dimensionSpecs,
      limitSpec,
      having,
      granularity,
      filter,
      aggregations,
      postAggregations,
      null,
      context
    ).setSegIntervals(segIns)

  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))
}

case class GroupByQuerySpecWithSegIntervals(
                             val queryType: String,
                             val dataSource: String,
                             override val dimensions: List[DimensionSpec],
                             val limitSpec: Option[LimitSpec],
                             val having: Option[HavingSpec],
                             val granularity: Either[String,GranularitySpec],
                             val filter: Option[FilterSpec],
                             override val aggregations: List[AggregationSpec],
                             override val postAggregations: Option[List[PostAggregationSpec]],
                             val intervals: SegmentIntervals,
                             override val context : Option[QuerySpecContext]
                           ) extends AggQuerySpec {

  override def intervalList: List[String] = intervals.segments.map(_.itvl)

  override def setSegIntervals(segInAssignments: List[(DruidSegmentInfo, Interval)]): QuerySpec = {
    this.copy(intervals = new SegmentIntervals(segInAssignments))
  }

  override def setIntervals(ins: List[Interval]): QuerySpec = ???
  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))
}

case class TimeSeriesQuerySpec(
                                val queryType: String,
                                val dataSource: String,
                                val intervals: List[String],
                                val granularity: Either[String,GranularitySpec],
                                val filter: Option[FilterSpec],
                                override val aggregations: List[AggregationSpec],
                                override val postAggregations: Option[List[PostAggregationSpec]],
                                override val context : Option[QuerySpecContext]
                                ) extends AggQuerySpec {
  def this(dataSource: String,
           intervals: List[String],
           granularity: Either[String,GranularitySpec],
           filters: Option[FilterSpec],
           aggregations: List[AggregationSpec],
           postAggregations: Option[List[PostAggregationSpec]],
           context : Option[QuerySpecContext]) = this("timeseries",
    dataSource, intervals, granularity, filters, aggregations, postAggregations, context)

  def setIntervals(ins : List[Interval]) = this.copy(intervals = ins.map(_.toString))
  def intervalList: List[String] = intervals

  def setSegIntervals(segIns : List[(DruidSegmentInfo, Interval)]) : QuerySpec =
    TimeSeriesQuerySpecWithSegIntervals(
      queryType,
      dataSource,
      granularity,
      filter,
      aggregations,
      postAggregations,
      null,
      context
    ).setSegIntervals(segIns)

  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))
}

case class TimeSeriesQuerySpecWithSegIntervals(
                                             val queryType: String,
                                             val dataSource: String,
                                             val granularity: Either[String,GranularitySpec],
                                             val filter: Option[FilterSpec],
                                             override val aggregations: List[AggregationSpec],
                                             override val postAggregations:
                                             Option[List[PostAggregationSpec]],
                                             val intervals: SegmentIntervals,
                                             override val context : Option[QuerySpecContext]
                                           ) extends AggQuerySpec {
  override def intervalList: List[String] = intervals.segments.map(_.itvl)

  override def setSegIntervals(segInAssignments: List[(DruidSegmentInfo, Interval)]): QuerySpec = {
    this.copy(intervals = new SegmentIntervals(segInAssignments))
  }

  override def setIntervals(ins: List[Interval]): QuerySpec = ???
  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))
}

case class TopNQuerySpec(
                          val queryType: String,
                          val dataSource: String,
                          val intervals: List[String],
                          val granularity: Either[String,GranularitySpec],
                          val filter: Option[FilterSpec],
                          override val aggregations: List[AggregationSpec],
                          override val postAggregations: Option[List[PostAggregationSpec]],
                          val dimension: DimensionSpec,
                          val threshold: Int,
                          val metric: TopNMetricSpec,
                          override val context : Option[QuerySpecContext]
                          ) extends AggQuerySpec {
  def this(dataSource: String,
           intervals: List[String],
           granularity: Either[String,GranularitySpec],
           filter: Option[FilterSpec],
           aggregations: List[AggregationSpec],
           postAggregations: Option[List[PostAggregationSpec]],
           dimension: DimensionSpec,
           threshold: Int,
           metric: TopNMetricSpec,
           context : Option[QuerySpecContext]) = this("topN", dataSource,
    intervals, granularity, filter, aggregations,
    postAggregations, dimension, threshold, metric, context)

  def setIntervals(ins : List[Interval]) = this.copy(intervals = ins.map(_.toString))
  def intervalList: List[String] = intervals

  def setSegIntervals(segIns : List[(DruidSegmentInfo, Interval)]) : QuerySpec =
    TopNQuerySpecWithSegIntervals(
      queryType,
      dataSource,
      null,
      granularity,
      filter,
      aggregations,
      postAggregations,
      dimension,
      threshold,
      metric,
      context
    ).setSegIntervals(segIns)

  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))

  override def dimensions : List[DimensionSpec] =
    List(dimension)

  override def apply(useSmile : Boolean,
                     is : InputStream,
                     druidQuerySvrConn: DruidClient,
                     onDone : => Unit = (),
                     fromList : Boolean = false) : CloseableIterator[ResultRow] =
    DruidTopNResultIterator(useSmile, is, onDone, fromList)
}

case class TopNQuerySpecWithSegIntervals(
                          val queryType: String,
                          val dataSource: String,
                          val intervals: SegmentIntervals,
                          val granularity: Either[String,GranularitySpec],
                          val filter: Option[FilterSpec],
                          override val aggregations: List[AggregationSpec],
                          override val postAggregations: Option[List[PostAggregationSpec]],
                          val dimension: DimensionSpec,
                          val threshold: Int,
                          val metric: TopNMetricSpec,
                          override val context : Option[QuerySpecContext]
                        ) extends AggQuerySpec {
  def this(dataSource: String,
           intervals: SegmentIntervals,
           granularity: Either[String,GranularitySpec],
           filter: Option[FilterSpec],
           aggregations: List[AggregationSpec],
           postAggregations: Option[List[PostAggregationSpec]],
           dimension: DimensionSpec,
           threshold: Int,
           metric: TopNMetricSpec,
           context : Option[QuerySpecContext]) = this("topN", dataSource,
    intervals, granularity, filter, aggregations,
    postAggregations, dimension, threshold, metric, context)

  def setIntervals(ins : List[Interval]) = ???
  def intervalList: List[String] = intervals.segments.map(_.itvl)

  def setSegIntervals(segInAssignments : List[(DruidSegmentInfo, Interval)]) : QuerySpec = {
    this.copy(intervals = new SegmentIntervals(segInAssignments))
  }
  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))

  override def dimensions : List[DimensionSpec] =
    List(dimension)

  override def apply(useSmile : Boolean,
                     is : InputStream,
                     druidQuerySvrConn: DruidClient,
                     onDone : => Unit = (),
                     fromList : Boolean = false) : CloseableIterator[ResultRow] =
    DruidTopNResultIterator(useSmile, is, onDone, fromList)

}

case class SearchQuerySpec(
                            val queryType: String,
                            val dataSource: String,
                            val intervals: List[String],
                            val granularity: Either[String,GranularitySpec],
                            val filter: Option[FilterSpec],
                            val searchDimensions : List[String],
                            val query : SearchQueryQuerySpec,
                            val limit : Int,
                            val sort : Option[SortSearchQuerySpec],
                            override val context : Option[QuerySpecContext]
                          ) extends AggQuerySpec {

  def this(dataSource: String,
           intervals: List[String],
           granularity: Either[String,GranularitySpec],
           filter: Option[FilterSpec],
           searchDimensions : List[String],
           query : SearchQueryQuerySpec,
           limit : Int,
           sort : Option[SortSearchQuerySpec],
           context : Option[QuerySpecContext]
  ) = this("search", dataSource, intervals, granularity,
    filter, searchDimensions, query, limit, sort, context)

  override def intervalList: List[String] = intervals

  override def setSegIntervals(segIns: List[(DruidSegmentInfo, Interval)]): QuerySpec =
    SearchQuerySpecWithSegIntervals(
      queryType,
      dataSource,
      null,
      granularity,
      filter,
      searchDimensions,
      query,
      limit,
      sort,
      context
    ).setSegIntervals(segIns)

  override def setIntervals(ins: List[Interval]): QuerySpec =
    this.copy(intervals = ins.map(_.toString))

  override def setFilter(fSpec: FilterSpec): QuerySpec = this.copy(filter = Some(fSpec))

  override def apply(useSmile : Boolean,
                     is : InputStream,
                     druidQuerySvrConn: DruidClient,
                     onDone : => Unit = (),
                     fromList : Boolean = false) : CloseableIterator[QueryResultRow] =
    new SearchQueryResultIterator(useSmile, is, onDone)


  override def dimensions : List[DimensionSpec] =
    searchDimensions.map(d => new DefaultDimensionSpec(d, d))

}

case class SearchQuerySpecWithSegIntervals(
                            val queryType: String,
                            val dataSource: String,
                            val intervals: SegmentIntervals,
                            val granularity: Either[String,GranularitySpec],
                            val filter: Option[FilterSpec],
                            val searchDimensions : List[String],
                            val query : SearchQueryQuerySpec,
                            val limit : Int,
                            val sort : Option[SortSearchQuerySpec],
                            override val context : Option[QuerySpecContext]
                          ) extends AggQuerySpec {


  def this(dataSource: String,
           intervals: SegmentIntervals,
           granularity: Either[String,GranularitySpec],
           filter: Option[FilterSpec],
           searchDimensions : List[String],
           query : SearchQueryQuerySpec,
           limit : Int,
           sort : Option[SortSearchQuerySpec] = None,
           context : Option[QuerySpecContext]
          ) =
    this("search", dataSource, intervals, granularity,
      filter, searchDimensions, query, limit, sort, context)

  override def intervalList: List[String] = intervals.segments.map(_.itvl)

  override def setSegIntervals(segInAssignments: List[(DruidSegmentInfo, Interval)]): QuerySpec = {
    this.copy(intervals = new SegmentIntervals(segInAssignments))
  }

  override def setIntervals(ins: List[Interval]): QuerySpec = ???

  override def setFilter(fSpec: FilterSpec): QuerySpec = this.copy(filter = Some(fSpec))

  override def apply(useSmile : Boolean,
                     is : InputStream,
                     druidQuerySvrConn: DruidClient,
                     onDone : => Unit = (),
                     fromList : Boolean = false) : CloseableIterator[QueryResultRow] =
    new SearchQueryResultIterator(useSmile, is, onDone)

  override def dimensions : List[DimensionSpec] =
    searchDimensions.map(n => new DefaultDimensionSpec(n,n))
}

case class PagingSpec(pagingIdentifiers : Map[String, Int],
                      threshold : Int,
                      fromNext : Option[Boolean] = None)

abstract class SelectSpec extends QuerySpec {
  self : Product =>
  def withPagingIdentifier(ps :  Map[String, Int]) : SelectSpec

  def dimensions : List[String]
  override def dimensionSpecs : List[DimensionSpec] =
    dimensions.map(n => new DefaultDimensionSpec(n,n))
  def metrics : List[String]

  def schemaFromQuerySpec(dInfo : DruidRelationInfo) : StructType = {
    val fields : List[StructField] = dimensions.map{ d =>
      val dc = dInfo.druidDS.columns(d)
      new StructField(d, DruidDataType.sparkDataType(dc.dataType))
    } ++ metrics.map{d =>
      val dc = dInfo.druidDS.columns(d)
      new StructField(d, DruidDataType.sparkDataType(dc.dataType))
    }
    StructType(fields)
  }

  override def apply(useSmile : Boolean,
                     is : InputStream,
                     druidQuerySvrConn: DruidClient,
                     onDone : => Unit = (),
                     fromList : Boolean = false) : CloseableIterator[ResultRow] =
    DruidSelectResultIterator(useSmile, is, this,
      druidQuerySvrConn.asInstanceOf[DruidQueryServerClient], onDone, fromList
    )

  override def mapSparkColNameToDruidColName(druidRelationInfo: DruidRelationInfo) :
  Map[String, String] =
    druidRelationInfo.sourceToDruidMapping.mapValues{
      case dc if dc.name == DruidDataSource.TIME_COLUMN_NAME =>
        DruidDataSource.EVENT_TIMESTAMP_KEY_NAME
      case dc => dc.name
    }

  def descending : Boolean

}

case class SelectSpecWithIntervals(queryType: String,
                                   dataSource: String,
                                   dimensions: List[String],
                                   metrics: List[String],
                                   filter: Option[FilterSpec],
                                   pagingSpec : PagingSpec,
                                   intervals: List[String] = List(),
                                   descending : Boolean = false,
                                   granularity : String = "all",
                                   override val context : Option[QuerySpecContext])
  extends SelectSpec {

  def this(dataSource: String,
           dimensions: List[String],
           metrics: List[String],
           filter: Option[FilterSpec],
           pagingSpec : PagingSpec,
           intervals: List[String] = List(),
           descending : Boolean = false,
           granularity : String = "all",
           context : Option[QuerySpecContext]) = this(
    "select", dataSource, dimensions, metrics, filter, pagingSpec,
    intervals, descending, granularity, context
  )

  def withPagingIdentifier(ps :  Map[String, Int]) = {
    copy(pagingSpec = pagingSpec.copy(pagingIdentifiers = ps, fromNext = Some(true)))
  }

  override def setIntervals(ins : List[Interval]) = this.copy(intervals = ins.map(_.toString))

  override def setSegIntervals(segIns : List[(DruidSegmentInfo, Interval)]) : QuerySpec =
    SelectSpecWithSegmentIntervals(
      queryType,
      dataSource,
      dimensions,
      metrics,
      filter,
      pagingSpec,
      null,
      descending,
      granularity,
      context
    ).setSegIntervals(segIns)

  def intervalList: List[String] = intervals

  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))
}

case class SelectSpecWithSegmentIntervals(queryType: String,
                                          dataSource: String,
                                          dimensions: List[String],
                                          metrics: List[String],
                                          filter: Option[FilterSpec],
                                          pagingSpec : PagingSpec,
                                          intervals: SegmentIntervals,
                                          descending : Boolean = false,
                                          granularity : String = "all",
                                          override val context : Option[QuerySpecContext])
  extends SelectSpec {

  def withPagingIdentifier(ps :  Map[String, Int]) = {
    copy(pagingSpec = pagingSpec.copy(pagingIdentifiers = ps, fromNext = Some(true)))
  }

  override def setSegIntervals(segInAssignments: List[(DruidSegmentInfo, Interval)]): QuerySpec = {
    this.copy(intervals = new SegmentIntervals(segInAssignments))
  }

  override def setIntervals(ins: List[Interval]): QuerySpec = ???

  override def intervalList: List[String] = intervals.segments.map(_.itvl)

  def setFilter(fSpec : FilterSpec) : QuerySpec = this.copy(filter = Some(fSpec))

}

/**
  * Used for testing only.
  */
case class DummyQuerySpec(val queryType : String = null) extends QuerySpec {

  override val dataSource: String = null

  override def intervalList: List[String] = ???

  override def setIntervals(ins: List[Interval]): QuerySpec = ???

  override def setSegIntervals(segIns: List[(DruidSegmentInfo, Interval)]): QuerySpec = ???

  override def filter: Option[FilterSpec] = ???

  override def setFilter(fSpec: FilterSpec): QuerySpec = ???

  override def context: Option[QuerySpecContext] = ???

  override def schemaFromQuerySpec(dInfo: DruidRelationInfo): StructType = ???

  def apply(useSmile : Boolean,
            is : InputStream,
            druidQuerySvrConn: DruidClient,
            onDone : => Unit = (),
            fromList : Boolean = false) : CloseableIterator[ResultRow] =
    new DummyResultIterator()
}