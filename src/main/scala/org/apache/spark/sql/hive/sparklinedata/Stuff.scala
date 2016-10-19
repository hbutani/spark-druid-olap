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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{FunctionResourceLoader, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveSessionCatalog, HiveSessionState, HiveSharedState}
import org.apache.spark.sql.internal.{SQLConf, SessionState, SharedState}
import org.apache.spark.sql.sparklinedata.ModuleLoader
import org.sparklinedata.druid.metadata.{DruidMetadataViews, DruidRelationInfo}


class SPLSessionCatalog(
                          externalCatalog: HiveExternalCatalog,
                          client: HiveClient,
                          sparkSession: SparkSession,
                          functionResourceLoader: FunctionResourceLoader,
                          functionRegistry: FunctionRegistry,
                          conf: SQLConf,
                          hadoopConf: Configuration)  extends
  HiveSessionCatalog(externalCatalog, client, sparkSession,
    functionResourceLoader, functionRegistry,
    conf, hadoopConf) {

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

  override def lookupRelation(
                               tableIdent: TableIdentifier,
                               alias: Option[String]): LogicalPlan = {
    val tableName = tableIdent.table
    DruidMetadataViews.metadataDFs.get(tableName).map{ f =>
      f(sparkSession.sqlContext).queryExecution.logical
    }.getOrElse(super.lookupRelation(tableIdent, alias))
  }
}

class SPLSessionState(sparkSession: SparkSession)
  extends HiveSessionState(sparkSession) {

  {
    /* Follow same procedure as SQLContext to add sparkline properties
     * to SQLContext.conf
     *
     */
    import scala.collection.JavaConverters._
    val properties = new Properties
    sparkSession.sparkContext.getConf.getAll.foreach {
      case (key, value) if key.startsWith("spark.sparklinedata") =>
        properties.setProperty(key, value)
      case _ =>
    }
    conf.setConf(properties)
  }

  val moduleLoader = ModuleLoader(sparkSession)
  moduleLoader.registerFunctions
  moduleLoader.addPhysicalRules
  moduleLoader.addLogicalRules

  private lazy val splSharedState: HiveSharedState = {
    sparkSession.sharedState.asInstanceOf[HiveSharedState]
  }

  override lazy val catalog = {
    new SPLSessionCatalog(
      splSharedState.externalCatalog,
      metadataHive,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf())
  }

  override lazy val sqlParser: ParserInterface = {
    new SPLParser(sparkSession,
      new SparkSqlParser(conf),
      moduleLoader.parsers,
      moduleLoader.parserTransformers
    )

  }

}

object SPLSessionState {

  def splCatalog(sqlContext : SQLContext) : SPLSessionCatalog = {
    sqlContext.sparkSession.sessionState.catalog.asInstanceOf[SPLSessionCatalog]
  }
}
