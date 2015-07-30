package org.sparklinedata.druid

import java.util.concurrent.atomic.AtomicLong

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DataType
import org.joda.time.Interval
import org.sparklinedata.druid.metadata.{DruidColumn, DruidRelationInfo}

/**
 *
 * @param drInfo
 * @param queryIntervals
 * @param dimensions
 * @param limitSpec
 * @param havingSpec
 * @param granularitySpec
 * @param filterSpec
 * @param aggregations
 * @param postAggregations
 * @param projectionAliasMap map from projected alias name to underlying column name.
 * @param outputAttributeMap list of output Attributes with the ExprId of the Attribute they
 *                           represent, the DataType in the original Plan and the DataType
 *                           from Druid.
 * @param curId
 */
case class DruidQueryBuilder(val drInfo : DruidRelationInfo,
                             queryIntervals: QueryIntervals,
                             dimensions: List[DimensionSpec] = Nil,
                             limitSpec: Option[LimitSpec] = None,
                             havingSpec : Option[HavingSpec] = None,
                             granularitySpec: Either[String,GranularitySpec] = Left("all"),
                             filterSpec: Option[FilterSpec] = None,
                             aggregations: List[AggregationSpec] = Nil,
                             postAggregations: Option[List[PostAggregationSpec]] = None,
                            projectionAliasMap : Map[String, String] = Map(),
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

  def filter( f : FilterSpec) = filterSpec match {
    case Some(f1 : FilterSpec) =>
      this.copy(filterSpec = Some(LogicalFilterSpec("and", List(f1, f))))
    case None => this.copy(filterSpec = Some(f))
  }

  def aggregate( a : AggregationSpec) = {
    this.copy(aggregations = ( aggregations :+ a))
  }

  def postAggregate( p : PostAggregationSpec) = postAggregations match {
    case None => this.copy(postAggregations = Some(List(p)))
    case Some(pAs) => this.copy(postAggregations = Some( pAs :+ p))
  }

  def interval(iC : IntervalCondition) : Option[DruidQueryBuilder] = iC.typ match {
    case IntervalConditionType.LT =>
      queryIntervals.ltCond(iC.dt).map(qI => this.copy(queryIntervals = qI))
    case IntervalConditionType.LTE =>
      queryIntervals.ltECond(iC.dt).map(qI => this.copy(queryIntervals = qI))
    case IntervalConditionType.GT =>
      queryIntervals.gtCond(iC.dt).map(qI => this.copy(queryIntervals = qI))
    case IntervalConditionType.GTE =>
      queryIntervals.gtECond(iC.dt).map(qI => this.copy(queryIntervals = qI))
  }

  def outputAttribute(nm : String, e : Expression, originalDT: DataType, druidDT : DataType) = {
    this.copy(outputAttributeMap = outputAttributeMap + (nm -> (e, originalDT, druidDT)))
  }

  def aggregateOp(op : Aggregate) = this.copy(aggregateOper = Some(op))

  def nextAlias : String = s"alias${curId.getAndDecrement()}"

  def druidColumn(name : String) : Option[DruidColumn] = {
    drInfo.sourceToDruidMapping.get(projectionAliasMap.getOrElse(name, name))
  }

  def addAlias(alias : String, col : String) = {
    val dColNm = projectionAliasMap.getOrElse(col, col)
    this.copy(projectionAliasMap = (projectionAliasMap + (alias -> dColNm)))
  }

}

object DruidQueryBuilder {
  def apply(drInfo : DruidRelationInfo) =
    new DruidQueryBuilder(drInfo, new QueryIntervals(drInfo))
}
