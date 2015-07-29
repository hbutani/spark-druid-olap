package org.sparklinedata.druid

import org.apache.spark.Logging
import org.json4s.jackson.JsonMethods._
import org.json4s.{Extraction, ShortTypeHints, FullTypeHints, DefaultFormats}
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.sparklinedata.druid.client.QueryResultRowSerializer
import org.sparklinedata.druid.metadata.FunctionalDependencyType

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
        classOf[TopNQuerySpec]
      )
    )
  ) +
    new EnumNameSerializer(FunctionalDependencyType) + new QueryResultRowSerializer ++
    org.json4s.ext.JodaTimeSerializers.all

  def logQuery(dq : DruidQuery) : Unit = {
    log.info(pretty(render(Extraction.decompose(dq))))
  }
}
