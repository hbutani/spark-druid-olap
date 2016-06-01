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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.sparklinedata.druid.metadata.DruidRelationInfo
import org.sparklinedata.druid.{DruidQuery, DruidRelation, QuerySpec, Utils}
import org.json4s._
import org.json4s.jackson.JsonMethods._


object PlanUtil {

  import org.json4s.JsonDSL._
  import Utils._

  def druidRelationInfo(tableName : String)(implicit sqlContext : SQLContext) :
  Option[DruidRelationInfo] = {
    sqlContext.table(tableName).logicalPlan.collectFirst {
      case LogicalRelation(DruidRelation(drInfo,_), _) => drInfo
    }
  }

  def dataFrame(drInfo : DruidRelationInfo, dq : DruidQuery)(
    implicit sqlContext : SQLContext) : DataFrame = {
    val dR = DruidRelation(drInfo, Some(dq))(sqlContext)
    val lP = LogicalRelation(dR, None)
    new DataFrame(sqlContext, lP)
  }

  @throws(classOf[AnalysisException])
  def logicalPlan(dsName : String, dqStr : String, usingHist : Boolean)(
    implicit sqlContext : SQLContext) : LogicalPlan = {
    val drInfo = druidRelationInfo(dsName)
    if (!drInfo.isDefined) {
      throw new AnalysisException(s"Cannot execute a DruidQuery on $dsName")
    }
    val dq = new DruidQuery(parse(dqStr).extract[QuerySpec], usingHist)
    val dR = DruidRelation(drInfo.get, Some(dq))(sqlContext)
    LogicalRelation(dR, None)
  }

}
