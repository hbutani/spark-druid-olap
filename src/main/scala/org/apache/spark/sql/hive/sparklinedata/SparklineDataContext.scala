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

package org.apache.spark.sql.hive.sparklinedata

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.catalyst.{ParserDialect, SqlParser, TableIdentifier}
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreCatalog, HiveQLDialect}
import org.apache.spark.sql.hive.client.{ClientInterface, ClientWrapper}
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.sparklinedata.druid.metadata.{DruidMetadataCache, DruidMetadataViews}


class SparklineDataContext(
                            sc: SparkContext,
                            cacheManager: CacheManager,
                            listener: SQLListener,
                            execHive: ClientWrapper,
                            metaHive: ClientInterface,
                            isRootContext: Boolean)
  extends HiveContext(
    sc,
    cacheManager,
    listener,
    execHive,
    metaHive,
    isRootContext) with Logging {

  DruidPlanner(this)

  def this(sc: SparkContext) = {
    this(sc, new CacheManager, SQLContext.createListenerAndUI(sc), null, null, true)
  }
  def this(sc: JavaSparkContext) = this(sc.sc)

  protected[sql] override def getSQLDialect(): ParserDialect = {
    new SparklineDataDialect(this)
  }

  override def newSession(): HiveContext = {
    new SparklineDataContext(
      sc = sc,
      cacheManager = cacheManager,
      listener = listener,
      execHive = executionHive.newSession(),
      metaHive = metadataHive.newSession(),
      isRootContext = false)
  }

  override protected[sql] lazy val catalog =
    new SparklineMetastoreCatalog(metadataHive, this) with OverrideCatalog
}

class SparklineMetastoreCatalog(client: ClientInterface, hive: HiveContext) extends
  HiveMetastoreCatalog(client: ClientInterface, hive: HiveContext) {

  override def lookupRelation(
                               tableIdent: TableIdentifier,
                               alias: Option[String]): LogicalPlan = {
    val tableName = tableIdent.table
    DruidMetadataViews.metadataDFs.get(tableName).map{ f =>
      f(hive).queryExecution.logical
    }.getOrElse(super.lookupRelation(tableIdent, alias))
  }
}
