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

package org.apache.spark.sql.sparklinedata.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.druid.{DruidPlanner, DruidQueryCostModel}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PlanUtil
import org.apache.spark.sql.{Row, SQLContext}
import org.sparklinedata.druid.metadata.DruidMetadataCache

case class ClearMetadata(druidHost: Option[String]) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("", StringType, nullable = true) :: Nil)

    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    if (druidHost.isDefined) {
      DruidMetadataCache.clearCache(druidHost.get)
    } else {
      DruidMetadataCache.clearCache
    }
    Seq(Row(""))
  }
}

case class ExplainDruidRewrite(sql: String) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("", StringType, nullable = true) :: Nil)

    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val qe = sqlContext.executeSql(sql)

    qe.sparkPlan.toString().split("\n").map(Row(_)).toSeq ++
    Seq(Row("")) ++
    DruidPlanner.getDruidRDDs(qe.sparkPlan).flatMap { dR =>
      val drInfo = PlanUtil.druidRelationInfo(dR.drFullName.sparkDataSource)(sqlContext).get
      s"""DruidQuery(${System.identityHashCode(dR.dQuery)}) details ::
         |${DruidQueryCostModel.computeMethod(sqlContext, drInfo, dR.dQuery.q)}
       """.stripMargin.split("\n").map(Row(_))
    }
  }
}


