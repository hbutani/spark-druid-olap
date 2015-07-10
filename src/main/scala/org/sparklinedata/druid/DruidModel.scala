package org.sparklinedata.druid

sealed trait ExtractionFunctionSpec {
  val `type`: String
}

/**
 * In SQL this is an 'like' or rlike' predicate on a column being grouped on.
 * @param `type`
 * @param expr
 */
case class RegexExtractionFunctionSpec(val `type`: String, val expr: String)
  extends ExtractionFunctionSpec

/**
 * In SQL this is a grouping expression of the form 'if col rlike regex then regex else null'
 * @param `type`
 * @param expr
 */
case class PartialExtractionFunctionSpec(val `type`: String, val expr: String)
  extends ExtractionFunctionSpec

/**
 * In SQL this is a contains predicate on a column being grouped on.
 * @param `type`
 * @param query
 */
case class SearchQueryExtractionFunctionSpec(val `type`: String, val query: String)
  extends ExtractionFunctionSpec

/**
 * In SQL this is a withTimeZone and field extraction functions applied to the time dimension
 * columns. Assume time functions are expressed using
 * [[https://github.com/SparklineData/spark-datetime SparklineData-sparkdatetime package]].
 * @param `type`
 * @param format
 * @param timeZone
 * @param locale
 */
case class TimeFormatExtractionFunctionSpec(val `type`: String,
                                            val format: String,
                                            val timeZone: String,
                                            val locale: String)
  extends ExtractionFunctionSpec

case class TimeParsingExtractionFunctionSpec(val `type`: String,
                                             val timeFormat: String,
                                             val resultFormat: String)
  extends ExtractionFunctionSpec

case class JavaScriptExtractionFunctionSpec(val `type`: String,
                                            val `function`: String,
                                            val injective: Boolean = false)
  extends ExtractionFunctionSpec

// TODO ExplicitLookupExtractionFunctionSpec

/**
 * As defined in [[http://druid.io/docs/latest/querying/dimensionspecs.html]]
 */
sealed trait DimensionSpec {
  val `type`: String
  val dimension: String
  val outputName: String
}

/**
 * In SQL these are columns being grouped on.
 * @param `type`
 * @param dimension
 * @param outputName
 */
case class DefaultDimensionSpec(val `type`: String, val dimension: String,
                                val outputName: String) extends DimensionSpec

case class ExtractionDimensionSpec(val `type`: String,
                                   val dimension: String,
                                   val outputName: String,
                                   extractionFn: ExtractionFunctionSpec) extends DimensionSpec

sealed trait GranularitySpec {
  val `type`: String
}

case class DurationGranularitySpec(`type`: String, duration: Long) extends GranularitySpec

case class PeriodGranularitySpec(`type`: String, period: String,
                                 timeZone: Option[String],
                                 origin: Option[String]) extends GranularitySpec

sealed trait FilterSpec {
  val `type`: String
}

case class SelectorFilterSpec(`type`: String,
                              dimension: String,
                              value: String) extends FilterSpec

case class RegexFilterSpec(`type`: String,
                           dimension: String,
                           pattern: String) extends FilterSpec

case class LogicalFilterSpec(`type`: String,
                             dimension: String,
                             fields: List[FilterSpec]) extends FilterSpec

case class NotFilterSpec(`type`: String,
                         dimension: String,
                         field: FilterSpec) extends FilterSpec

/**
 * In SQL an invocation on a special JSPredicate is translated to this FilterSpec.
 * JSPredicate has the signature jspredicate(colum, jsFuncCodeAsString)
 * @param `type`
 * @param dimension
 * @param function
 */
case class JavascriptFilterSpec(`type`: String,
                                dimension: String,
                                function: String) extends FilterSpec

sealed trait AggregationSpec {
  val `type`: String
}

/**
 * In SQL an aggregation expression on a metric is translated to this Spec.
 * @param `type` can be "count", "longSum", "doubleSum", "min", "max", "hyperUnique"
 * @param name
 * @param fieldName
 */
case class FunctionAggregationSpec(val `type`: String,
                                   val name: String,
                                   val fieldName: Option[String]
                                    ) extends AggregationSpec

/**
 * In SQL a count(distinct dimColumn) is translated to this Spec.
 * @param `type`
 * @param name
 * @param fieldNames
 * @param byRow
 */
case class CardinalityAggregationSpec(val `type`: String,
                                      val name: String,
                                      val fieldNames: List[String],
                                      val byRow: Boolean
                                       ) extends AggregationSpec

/**
 * In SQL this is an invocation on a special JSAgg function. Its signature is
 * JSAggLong(metricCol, aggFnCode, combineFnCode, resetFnCode). Similar kind of
 * function double metrics: JSAggDouble.
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
                                      ) extends AggregationSpec

/**
 * In SQL this is an aggregation on an If condition. For example
 * sum(if dimCol = value then metric else null end)
 * @param `type`
 * @param filter
 * @param aggregator
 */
case class FilteredAggregationSpec(val `type`: String,
                                   val filter: SelectorFilterSpec,
                                   val aggregator: AggregationSpec
                                    ) extends AggregationSpec

sealed trait PostAggregationSpec {
  val `type`: String
}

case class FieldAccessPostAggregationSpec(val `type`: String,
                                          val fieldName: String
                                           ) extends AggregationSpec

case class ConstantPostAggregationSpec(val `type`: String,
                                       val name: String,
                                       val value: Double
                                        ) extends AggregationSpec

case class HyperUniqueCardinalityPostAggregationSpec(val `type`: String,
                                                     val fieldName: String
                                                      ) extends AggregationSpec

/**
 * In SQL this is an expression involving at least 1 aggregation expression
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
                                          ) extends AggregationSpec

case class JavascriptPostAggregationSpec(val `type`: String,
                                         val name: String,
                                         val fields: List[PostAggregationSpec],
                                         val function: String
                                          ) extends AggregationSpec

/**
 *
 * @param dimension
 * @param direction can be "ascending"|"descending"
 */
case class OrderByColumnSpec(val dimension: String,
                             val direction: String)

case class LimitSpec(val `type`: String,
                     limit: Int,
                     columns: List[OrderByColumnSpec])

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
                                  ) extends TopNMetricSpec

case class LexiographicTopNMetricSpec(
                                       val `type`: String,
                                       val previousStop: String
                                       ) extends TopNMetricSpec

case class AlphaNumericTopNMetricSpec(
                                       val `type`: String,
                                       val previousStop: String
                                       ) extends TopNMetricSpec

case class InvertedTopNMetricSpec(
                                   val `type`: String,
                                   val metric: TopNMetricSpec
                                   ) extends TopNMetricSpec

// TODO: look into exposing ContextSpec

case class GroupByQuery(
                         val queryType: String,
                         val dataSource: String,
                         val dimensions: List[DimensionSpec],
                         val limitSpec: Option[LimitSpec],
                         val having: Option[HavingSpec],
                         val granularity: GranularitySpec,
                         val filter: Option[FilterSpec],
                         val aggregations: List[AggregationSpec],
                         val postAggregations: Option[List[PostAggregationSpec]],
                         val intervals: List[String]
                         )

case class TimeSeriesQuery(
                            val queryType: String,
                            val dataSource: String,
                            val intervals: List[String],
                            val granularity: GranularitySpec,
                            val filters: Option[FilterSpec],
                            val aggregations: List[AggregationSpec],
                            val postAggregations: Option[List[PostAggregationSpec]]
                            )

case class TopNQuery(
                      val queryType: String,
                      val dataSource: String,
                      val intervals: List[String],
                      val granularity: GranularitySpec,
                      val filter: Option[FilterSpec],
                      val aggregations: List[AggregationSpec],
                      val postAggregations: Option[List[PostAggregationSpec]],
                      val dimension: DimensionSpec,
                      val threshold: Int,
                      val metric: TopNMetricSpec
                      )