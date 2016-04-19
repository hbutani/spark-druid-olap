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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DataType
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
 * @param aggExprToLiteralExpr for expressions that represent a 'null' value for
 *                             a this GroupingSet or represent the 'grouping__id'
 *                             columns, this is a map to the Literal value that is
 *                             filled in the Projection above the DruidRDD.
 * @param aggregateOper
 * @param curId
 */
case class DruidQueryBuilder(val drInfo: DruidRelationInfo,
                             queryIntervals: QueryIntervals,
                             dimensions: List[DimensionSpec] = Nil,
                             limitSpec: Option[LimitSpec] = None,
                             havingSpec: Option[HavingSpec] = None,
                             granularitySpec: Either[String, GranularitySpec] = Left("all"),
                             filterSpec: Option[FilterSpec] = None,
                             aggregations: List[AggregationSpec] = Nil,
                             postAggregations: Option[List[PostAggregationSpec]] = None,
                             projectionAliasMap: Map[String, String] = Map(),
                             outputAttributeMap:
                             Map[String, (Expression, DataType, DataType)] = Map(),
                             aggExprToLiteralExpr: Map[Expression, Expression] = Map(),
                             aggregateOper: Option[Aggregate] = None,
                             curId: AtomicLong = new AtomicLong(-1)) {
  def dimension(d: DimensionSpec) = {
    this.copy(dimensions = (dimensions :+ d))
  }

  def limit(l: LimitSpec) = {
    this.copy(limitSpec = Some(l))
  }

  def having(h: HavingSpec) = {
    this.copy(havingSpec = Some(h))
  }

  def granularity(g: GranularitySpec) = {
    this.copy(granularitySpec = Right(g))
  }

  def filter(f: FilterSpec) = filterSpec match {
    case Some(f1: FilterSpec) =>
      this.copy(filterSpec = Some(LogicalFilterSpec("and", List(f1, f))))
    case None => this.copy(filterSpec = Some(f))
  }

  def aggregate(a: AggregationSpec) = {
    this.copy(aggregations = (aggregations :+ a))
  }

  def postAggregate(p: PostAggregationSpec) = postAggregations match {
    case None => this.copy(postAggregations = Some(List(p)))
    case Some(pAs) => this.copy(postAggregations = Some(pAs :+ p))
  }

  def interval(iC: IntervalCondition): Option[DruidQueryBuilder] = iC.typ match {
    case IntervalConditionType.LT =>
      queryIntervals.ltCond(iC.dt).map(qI => this.copy(queryIntervals = qI))
    case IntervalConditionType.LTE =>
      queryIntervals.ltECond(iC.dt).map(qI => this.copy(queryIntervals = qI))
    case IntervalConditionType.GT =>
      queryIntervals.gtCond(iC.dt).map(qI => this.copy(queryIntervals = qI))
    case IntervalConditionType.GTE =>
      queryIntervals.gtECond(iC.dt).map(qI => this.copy(queryIntervals = qI))
  }

  def outputAttribute(nm: String, e: Expression, originalDT: DataType, druidDT: DataType) = {
    this.copy(outputAttributeMap = outputAttributeMap + (nm ->(e, originalDT, druidDT)))
  }

  def aggregateOp(op: Aggregate) = this.copy(aggregateOper = Some(op))

  def nextAlias: String = s"alias${curId.getAndDecrement()}"

  def nextAlias(cn: String): String = {
    var oAttrName = cn + nextAlias
    while (drInfo.sourceToDruidMapping.contains(oAttrName)) {
      oAttrName = cn + nextAlias
    }
    oAttrName
  }

  def druidColumn(name: String): Option[DruidColumn] = {
    drInfo.sourceToDruidMapping.get(projectionAliasMap.getOrElse(name, name))
  }

  def addAlias(alias: String, col: String) = {
    val dColNm = projectionAliasMap.getOrElse(col, col)
    this.copy(projectionAliasMap = (projectionAliasMap + (alias -> dColNm)))
  }

  def orderBy(dimName: String, ascending: Boolean): DruidQueryBuilder = limitSpec match {
    case Some(LimitSpec(t, l, columns)) => limit(LimitSpec(t, l,
      columns :+ new OrderByColumnSpec(dimName, ascending)))
    case None => limit(new LimitSpec(Int.MaxValue,
      new OrderByColumnSpec(dimName, ascending)))
  }

  def limit(amt: Int): Option[DruidQueryBuilder] = limitSpec match {
    case Some(LimitSpec(t, l, columns)) if (l == Int.MaxValue || l == amt) =>
      Some(limit(LimitSpec(t, amt, columns)))
    case _ => None
  }

}

object DruidQueryBuilder {
  def apply(drInfo: DruidRelationInfo) =
    new DruidQueryBuilder(drInfo, new QueryIntervals(drInfo))
}
