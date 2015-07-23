package org.sparklinedata.druid

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DataType
import org.joda.time.Interval
import org.sparklinedata.druid.metadata.DruidRelationInfo

/**
 *
 * @param drInfo
 * @param dimensions
 * @param limitSpec
 * @param havingSpec
 * @param granularitySpec
 * @param filterSpec
 * @param aggregations
 * @param postAggregations
 * @param intervals
 * @param outputAttributeMap list of output Attributes with the ExprId of the Attribute they
 *                           represent, the DataType in the original Plan and the DataType
 *                           from Druid.
 * @param curId
 */
case class DruidQueryBuilder(val drInfo : DruidRelationInfo,
                             dimensions: List[DimensionSpec] = Nil,
                             limitSpec: Option[LimitSpec] = None,
                             havingSpec : Option[HavingSpec] = None,
                             granularitySpec: Either[String,GranularitySpec] = Left("all"),
                             filterSpec: Option[FilterSpec] = None,
                             aggregations: List[AggregationSpec] = Nil,
                             postAggregations: Option[List[PostAggregationSpec]] = None,
                             intervals: Option[List[Interval]] = None,
                            outputAttributeMap :
                             Map[String, (Expression, DataType, DataType)] = Map(),
                            aggregateOper : Option[Aggregate] = None,
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

  def postAggregate( p : PostAggregationSpec) = postAggregations match {
    case None => this.copy(postAggregations = Some(List(p)))
    case Some(pAs) => this.copy(postAggregations = Some( pAs :+ p))
  }

  def interval(i : Interval) = intervals match {
    case None => this.copy(intervals = Some(List(i)))
    case Some(iS) => this.copy(intervals = Some(i +: iS))
  }

  def outputAttribute(nm : String, e : Expression, originalDT: DataType, druidDT : DataType) = {
    this.copy(outputAttributeMap = outputAttributeMap + (nm -> (e, originalDT, druidDT)))
  }

  def aggregateOp(op : Aggregate) = this.copy(aggregateOper = Some(op))

  def nextAlias : String = s"alias${curId.getAndDecrement()}"

}
