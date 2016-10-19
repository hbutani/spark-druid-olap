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

package org.sparklinedata.druid.metadata

import org.apache.spark.sql.hive.sparklinedata.SPLSessionState

import scala.language.implicitConversions
import org.apache.spark.sql.{DataFrame, SQLContext}

case class DruidColumnView(name: String,
                           dataType: String,
                           size: Long
                          )

object DruidColumnView {

  implicit def toDruidColumnView(dC: DruidColumn): DruidColumnView = {
    DruidColumnView(dC.name,
      dC.dataType.toString,
      dC.size
    )
  }
}

case class DruidDataSourceView(name: String,
                               intervals: List[String],
                               columns: Map[String, DruidColumnView],
                               size: Long)

object DruidDataSourceView {

  implicit def toDruidDataSourceView(dDS: DruidDataSource): DruidDataSourceView = {
    DruidDataSourceView(dDS.name,
      dDS.intervals.map(_.toString),
      dDS.columns.map {
        case (k, v) => (k, DruidColumnView.toDruidColumnView(v))
      },
      dDS.size
    )
  }
}

case class DruidRelationView(sparkDataSource: String,
                             druidHost: String,
                             druidDataSource: String,
                             val sourceDFName: String,
                             val timeDimensionCol: String,
                             intervals: List[String],
                             val maxCardinality: Long,
                             val cardinalityPerDruidQuery: Long,
                             pushHLLTODruid: Boolean,
                             streamDruidQueryResults: Boolean,
                             loadMetadataFromAllSegments: Boolean,
                             zkSessionTimeoutMs: Int,
                             zkEnableCompression: Boolean,
                             zkDruidPath: String,
                             queryHistoricalServers: Boolean,
                             zkQualifyDiscoveryNames: Boolean,
                             numSegmentsPerHistoricalQuery: Int,
                             columns: Seq[DruidColumnView],
                             val sourceToDruidMapping: Map[String, String])

object DruidRelationView {

  implicit def toDruidRelationInfoView(drRel: DruidRelationInfo): DruidRelationView = {
    val druidDSView: DruidDataSourceView = drRel.druidDS
    DruidRelationView(
      drRel.fullName.sparkDataSource,
      drRel.fullName.druidHost,
      drRel.fullName.druidDataSource,
      drRel.sourceDFName,
      drRel.timeDimensionCol,
      druidDSView.intervals,
      drRel.options.maxCardinality,
      drRel.options.cardinalityPerDruidQuery,
      drRel.options.pushHLLTODruid,
      drRel.options.streamDruidQueryResults,
      drRel.options.loadMetadataFromAllSegments,
      drRel.options.zkSessionTimeoutMs,
      drRel.options.zkEnableCompression,
      drRel.options.zkDruidPath,
      drRel.options.queryHistoricalServers,
      drRel.options.zkQualifyDiscoveryNames,
      drRel.options.numSegmentsPerHistoricalQuery,
      druidDSView.columns.values.toSeq,
      drRel.sourceToDruidMapping.map {
        case (k, v) => (k, v.name)
      }
    )
  }
}

case class DruidServerView(druidHost: String,
                           druidServer: String,
                           maxSize: Long,
                           serverType: String,
                           tier: String,
                           priority: Int,
                           numSegments: Int,
                           currSize: Long)

object DruidServerView {

  def apply(host: String, hSvr: HistoricalServerInfo): DruidServerView = {
    new DruidServerView(host,
      hSvr.host,
      hSvr.maxSize,
      hSvr.`type`,
      hSvr.tier,
      hSvr.priority,
      hSvr.segments.size,
      hSvr.currSize
    )
  }
}

case class DruidSegmentView(druidHost: String,
                            druidDataSource: String,
                            interval: String,
                            version: String,
                            binaryVersion: String,
                            size: Long,
                            identifier: String,
                            shardType: Option[String],
                            partitionNum: Option[Int],
                            partitions: Option[Int]
                           )

object DruidSegmentView {

  def apply(host: String,
            sInfo: DruidSegmentInfo,
            dDS: DruidDataSource): DruidSegmentView = {
    new DruidSegmentView(
      host,
      dDS.name,
      sInfo.interval,
      sInfo.version,
      sInfo.binaryVersion,
      sInfo.size,
      sInfo.identifier,
      sInfo.shardSpec.map(_.`type`),
      sInfo.shardSpec.flatMap(_.partitionNum),
      sInfo.shardSpec.flatMap(_.partitions)
    )
  }
}

case class DruidServerAssignmentView(druidHost: String,
                                     druidServer: String,
                                     segIdentifier: String
                                    )

object DruidMetadataViews {

  val metadataDFs = Map(
    "d$druidrelations" -> getDruidRelationView _,
    "d$druidservers" -> getDruidServersView _,
    "d$druidsegments" -> getDruidSegmentsView _,
    "d$druidserverassignments" -> getDruidServerAssignmentsView _,
    "d$druidqueries" -> getDruidQueryHistory _
  )

  def getDruidRelationView(sqlContext: SQLContext): DataFrame = {
    val l = SPLSessionState.splCatalog(sqlContext).druidRelations.map {
      DruidRelationView.toDruidRelationInfoView(_)
    }
    sqlContext.createDataFrame(l)
  }

  def getDruidServersView(sqlContext: SQLContext): DataFrame = {

    val l = (
      for ((n, cI) <- DruidMetadataCache.cache;
           hS <- cI.histServers
      ) yield DruidServerView(n, hS)
      ).toSeq

    sqlContext.createDataFrame(l)
  }

  def getDruidSegmentsView(sqlContext: SQLContext): DataFrame = {

    val l = (
      for ((n, cI) <- DruidMetadataCache.cache;
           (dsName, (segsInfo, dDS)) <- cI.druidDataSources;
           segInfo <- segsInfo.segments
      ) yield DruidSegmentView(n, segInfo, dDS)
      ).toSeq

    sqlContext.createDataFrame(l)
  }

  def getDruidServerAssignmentsView(sqlContext: SQLContext): DataFrame = {

    val l = (
      for ((n, cI) <- DruidMetadataCache.cache;
           hS <- cI.histServers;
           segInfo <- hS.segments.values
      ) yield DruidServerAssignmentView(n, hS.host, segInfo.identifier)
      ).toSeq

    sqlContext.createDataFrame(l)
  }

  def getDruidQueryHistory(sqlContext: SQLContext): DataFrame = {
    sqlContext.createDataFrame(DruidQueryHistory.getHistory)
  }

}

