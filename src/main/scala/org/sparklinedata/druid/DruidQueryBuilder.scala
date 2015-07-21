package org.sparklinedata.druid

import java.util.concurrent.atomic.AtomicLong

import org.sparklinedata.druid.metadata.DruidRelationInfo

case class DruidQueryBuilder(val drInfo : DruidRelationInfo,
                             dimensions: List[DimensionSpec] = Nil,
                             limitSpec: Option[LimitSpec] = None,
                             havingSpec : Option[HavingSpec] = None,
                             granularitySpec: Either[String,GranularitySpec] = Left("all"),
                             filterSpec: Option[FilterSpec] = None,
                             aggregations: List[AggregationSpec] = Nil,
                             postAggregations: List[PostAggregationSpec] = Nil,
                             intervals: List[String] = Nil,
                             curId : AtomicLong = new AtomicLong(-1)) {

  def dimension(d : DimensionSpec) = {
    this.copy(dimensions =  (dimensions :+ d))
  }

  def limit(l : LimitSpec) = {
    this.copy(limitSpec = Some(l))
  }

  def having(h : HavingSpec) = {
    this.copy(havingSpec = Some(h))
  }

  def granularity(g : GranularitySpec) = {
    this.copy(granularitySpec = Right(g))
  }

  def filter( f : FilterSpec) = {
    this.copy(filterSpec = Some(f))
  }

  def aggregate( a : AggregationSpec) = {
    this.copy(aggregations = ( aggregations :+ a))
  }

  def postAggregate( p : PostAggregationSpec) = {
    this.copy(postAggregations = ( postAggregations :+ p))
  }

  def interval(i : String) = {
    this.copy(intervals = (i +: intervals))
  }

  def nextAlias : String = s"alias${curId.getAndDecrement()}"

}
