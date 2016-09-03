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

import java.util.Properties

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{ParserDialect, TableIdentifier}
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.hive.client.{ClientInterface, ClientWrapper}
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreCatalog}
import org.apache.spark.sql.planner.logical.DruidLogicalOptimizer
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.{Logging, SparkContext}
import org.sparklinedata.druid.metadata.{DruidMetadataViews, DruidRelationInfo}


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

  {
    /* Follow same procedure as SQLContext to add sparkline properties
     * to SQLContext.conf
     *
     */
    import scala.collection.JavaConverters._
    val properties = new Properties
    sparkContext.getConf.getAll.foreach {
      case (key, value) if key.startsWith("spark.sparklinedata") =>
        properties.setProperty(key, value)
      case _ =>
    }
    conf.setConf(properties)
    properties.asScala.foreach {
      case (key, value) => setConf(key, value)
    }
  }

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

  override lazy val catalog =
    new SparklineMetastoreCatalog(metadataHive, this) with OverrideCatalog

  override protected[sql] lazy val optimizer: Optimizer = new DruidLogicalOptimizer(conf)
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

  def druidRelations : Seq[DruidRelationInfo] = {

    Seq()

//    TODO: fix this
//    Scala compiler generates a function invocation for the expression `cachedDataSourceTables`
//    since it implements the com.google.common.base.Function interface,
    // which has an apply method.
//
//    import collection.JavaConversions._
//
//    cachedDataSourceTables.asMap().values.collect {
//      case LogicalRelation(DruidRelation(info, _), _) => info
//    }.toSeq
  }
}
