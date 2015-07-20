package org.sparklinedata.druid

import org.sparklinedata.druid.metadata.DruidRelationInfo

class DruidQueryBuilder(val drInfo : DruidRelationInfo) {

  private var dimensions: List[DimensionSpec] = Nil
  private var limitSpec: Option[LimitSpec] = None
  private var havingSpec : Option[HavingSpec] = None
  private var granularitySpec: Either[String,GranularitySpec] = Left("all")
  private var filterSpec: Option[FilterSpec] = None
  private var aggregations: List[AggregationSpec] = Nil
  private var postAggregations: List[PostAggregationSpec] = Nil
  private var intervals: List[String] = Nil

  def dimension(d : DimensionSpec) = {
    dimensions =  (dimensions :+ d)
    this
  }

  def limit(l : LimitSpec) = {
    limitSpec = Some(l)
    this
  }

  def having(h : HavingSpec) = {
    havingSpec = Some(h)
    this
  }

  def granularity(g : GranularitySpec) = {
    granularitySpec = Right(g)
    this
  }

  def filter( f : FilterSpec) = {
    filterSpec = Some(f)
    this
  }

  def aggregate( a : AggregationSpec) = {
    aggregations = ( aggregations :+ a)
    this
  }

  def postAggregate( p : PostAggregationSpec) = {
    postAggregations = ( postAggregations :+ p)
    this
  }

  def interval(i : String) = {
    intervals = (i +: intervals)
    this
  }

}
