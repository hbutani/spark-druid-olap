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

package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.metadata.DruidRelationInfo
import org.sparklinedata.druid.{DruidQuery, DruidRelation, QuerySpec, Utils}


object PlanUtil {

  import Utils._

  def druidRelationInfo(tableName: String)(implicit sqlContext: SQLContext):
  Option[DruidRelationInfo] = {
    sqlContext.table(tableName).logicalPlan.collectFirst {
      case LogicalRelation(DruidRelation(drInfo, _), _) => drInfo
    }
  }

  def dataFrame(drInfo: DruidRelationInfo, dq: DruidQuery)(
    implicit sqlContext: SQLContext): DataFrame = {
    val dR = DruidRelation(drInfo, Some(dq))(sqlContext)
    val lP = LogicalRelation(dR, None)
    new DataFrame(sqlContext, lP)
  }

  @throws(classOf[AnalysisException])
  def logicalPlan(dsName: String, dqStr: String, usingHist: Boolean)(
    implicit sqlContext: SQLContext): LogicalPlan = {
    val drInfo = druidRelationInfo(dsName)
    if (!drInfo.isDefined) {
      throw new AnalysisException(s"Cannot execute a DruidQuery on $dsName")
    }
    val dq = new DruidQuery(parse(dqStr).extract[QuerySpec],
      drInfo.get.options.useSmile(sqlContext),
      usingHist,
      drInfo.get.options.numSegmentsPerHistoricalQuery(sqlContext))
    val dR = DruidRelation(drInfo.get, Some(dq))(sqlContext)
    LogicalRelation(dR, None)
  }

  /**
    * Get cardinality agumenters below the given node
    *
    * @param root Node below which to look for aggregates
    * @return
    */
  def getCardinalityAugmenters(root: LogicalPlan): Seq[LogicalPlan] = {
    root.collect {
      case j@Join(l, r, _, _) if !maxCardinalityIsOne(l) || !maxCardinalityIsOne(r) => j
      case u: Union => u
      case g: Generate => g
      case c: Cube => c
      case r: Rollup => r
      case gs: GroupingSets => gs
    }
  }

  /**
    * Is the given node a cardinality augmenter?
    *
    * @param lp Node to check for
    * @return
    */
  def cardinalityAugmenter(lp: LogicalPlan): Boolean = {
    lp match {
      case Join(l, r, _, _) => true
      case u: Union => true
      case g: Generate => true
      case c: Cube => true
      case r: Rollup => true
      case gs: GroupingSets => true
      case _ => false
    }
  }

  /**
    * Is cardinality augmented between given node (inclusive) & all of its children (exclusive)
    *
    * @param root Starting Node
    * @param children boundary nodes (exclusive)
    * @return
    */
  def isCardinalityAugmented(root: LogicalPlan, children: Seq[LogicalPlan]): Boolean = {
    cardinalityAugmenter(root) || {
      val cardinalityAugmenters = getCardinalityAugmenters(root)
      cardinalityAugmenters.exists(ca => children.forall(ch => ca.containsChild.contains(ch)))
    }
  }

  /**
    * Is the cardinality of given node one?
    *
    * @param lp node to check for
    * @return
    */
  def maxCardinalityIsOne(lp: LogicalPlan): Boolean = {
    var isone = false

    val aggs = lp.collect {case ag: Aggregate if ag.groupingExpressions.isEmpty => ag}
    if (aggs.nonEmpty) {
      isone = !isCardinalityAugmented(lp, aggs.asInstanceOf[Seq[LogicalPlan]])
    }
    isone
  }
}
