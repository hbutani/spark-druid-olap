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

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.SparklineThreadUtils
import org.joda.time.Interval
import org.sparklinedata.druid.DruidDataSourceException

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.mutable.{Map => MMap}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import org.sparklinedata.druid.client.{CuratorConnection, DruidCoordinatorClient, DruidQueryServerClient}


case class ModuleInfo(
                     name : String,
                     artifact : String,
                     version : String
                     )

case class ServerMemory(
                         maxMemory: Long,
                         totalMemory: Long,
                         freeMemory: Long,
                         usedMemory: Long
                       )

case class ServerStatus(
                        version : String,
                        modules : List[ModuleInfo],
                        memory : ServerMemory
                       )

case class DruidNode(name: String,
                     id: String,
                     address: String,
                     port: Int)

case class ShardSpec(
                      `type`: String,
                      partitionNum: Option[Int],
                      partitions: Option[Int]
                    )

case class DruidSegmentInfo(dataSource: String,
                            interval: String,
                            version: String,
                            binaryVersion: String,
                            size: Long,
                            identifier: String,
                            shardSpec: Option[ShardSpec]
                           ) {

  lazy val _interval = Interval.parse(interval)

  def overlaps(in: Interval) = _interval.overlaps(in)
}

case class HistoricalServerInfo(host: String,
                                maxSize: Long,
                                `type`: String,
                                tier: String,
                                priority: Int,
                                segments: Map[String, DruidSegmentInfo],
                                currSize: Long) {

  def handlesSegment(segId: String) = segments.contains(segId)
}

case class DataSourceSegmentInfo(name: String,
                                 properties: Map[String, String],
                                 segments: List[DruidSegmentInfo]
                                ) {

  def segmentsToScan(in: Interval): List[DruidSegmentInfo] = segments.filter(_.overlaps(in))

}

case class DruidClusterInfo(host: String,
                            curatorConnection: CuratorConnection,
                            coordinatorStatus : ServerStatus,
                            druidDataSources: scala.collection.Map[String,
                              (DataSourceSegmentInfo, DruidDataSource)],
                            histServers: List[HistoricalServerInfo]) {

  def historicalServers(dRName: DruidRelationName, ins: List[Interval]):
  List[HistoricalServerAssignment] = {

    val m: MMap[String, (HistoricalServerInfo, List[(DruidSegmentInfo, Interval)])] = MMap()

    /**
      * - favor the higher priority server
      * - when the priorities are equal
      * - favor the server with least work.
      */
    implicit val o = new Ordering[HistoricalServerInfo] {
      override def compare(x: HistoricalServerInfo, y: HistoricalServerInfo): Int = {
        if (x.priority == y.priority) {
          (m.get(x.host), m.get(y.host)) match {
            case (None, None) => 0 // x
            case (None, _) => -1 // x
            case (_, None) => 1 // y
            case (Some((i1, l1)), Some((i2, l2))) => l1.size - l2.size
          }

        } else {
          y.priority - x.priority
        }
      }
    }

    for (in <- ins) {
      druidDataSources(dRName.druidDataSource)._1.segmentsToScan(in).map { seg =>
        val segIn = seg._interval.overlap(in)
        val s = histServers.filter(_.handlesSegment(seg.identifier)).sorted.head
        if (m.contains(s.host)) {
          val v = m(s.host)
          val segInTuple = (seg, segIn)
          m(s.host) = (v._1, segInTuple :: v._2)
        } else {
          m(s.host) = (s, List((seg, segIn)))
        }
      }
    }

    m.map {
      case (h, t) => HistoricalServerAssignment(t._1, t._2)
    }.toList
  }

}

trait DruidMetadataCache {
  type DruidDataSourceInfo = (DataSourceSegmentInfo, DruidDataSource)

  def getDruidClusterInfo(druidRelName: DruidRelationName,
                          options: DruidRelationOptions): DruidClusterInfo

  def getDataSourceInfo(druidRelName: DruidRelationName,
                        options: DruidRelationOptions): DruidDataSourceInfo

  def assignHistoricalServers(druidRelName: DruidRelationName,
                              options: DruidRelationOptions,
                              intervals: List[Interval]): List[HistoricalServerAssignment]

  def register(druidRelName: DruidRelationName,
               options: DruidRelationOptions): Unit

  def clearCache(host: String): Unit
}

trait DruidRelationInfoCache {

  self: DruidMetadataCache =>

  // scalastyle:off parameter.number
  def druidRelation(sqlContext: SQLContext,
                             sourceDFName: String,
                             sourceDF: DataFrame,
                             dsName: String,
                             timeDimensionCol: String,
                             druidHost: String,
                             columnMapping: Map[String, String],
                             functionalDeps: List[FunctionalDependency],
                             starSchema: StarSchema,
                             options: DruidRelationOptions): DruidRelationInfo = {

    val fullName = DruidRelationName(starSchema.factTable.name, druidHost, dsName)

    val druidDS = getDataSourceInfo(
      fullName,
      options)._2
    val sourceToDruidMapping =
      MappingBuilder.buildMapping(sqlContext, sourceDFName,
        starSchema, columnMapping, timeDimensionCol, druidDS)
    val fd = new FunctionalDependencies(druidDS, functionalDeps,
      DependencyGraph(druidDS, functionalDeps))

    val dr = DruidRelationInfo(fullName,
      sourceDFName,
      timeDimensionCol,
      druidDS,
      sourceToDruidMapping,
      fd,
      starSchema,
      options)
    dr
  }

  def druidRelation(sQLContext: SQLContext,
                    registeredInfo : DruidRelationInfo) : DruidRelationInfo = {

    import registeredInfo._

    val druidDS = getDataSourceInfo(
      fullName,
      options)._2

    DruidRelationInfo(fullName,
      sourceDFName,
      timeDimensionCol,
      druidDS,
      sourceToDruidMapping,
      fd,
      starSchema,
      options)

  }
}

case class HistoricalServerAssignment(server: HistoricalServerInfo,
                                      segmentIntervals: List[(DruidSegmentInfo, Interval)])

object DruidMetadataCache extends DruidMetadataCache with DruidRelationInfoCache {

  private[metadata] val cache: MMap[String, DruidClusterInfo] = MMap()
  private val curatorConnections : MMap[String, CuratorConnection] = MMap()
  val thrdPool = SparklineThreadUtils.newDaemonCachedThreadPool("druidMD", 5)
  implicit val ec = ExecutionContext.fromExecutor(thrdPool)

  private def curatorConnection(host : String,
                                options: DruidRelationOptions) : CuratorConnection = {
    curatorConnections.getOrElse(host, {
      val cc = new CuratorConnection(host, options, this, this.thrdPool)
      curatorConnections(host) = cc
      cc
    }
    )
  }

  def getDruidClusterInfo(dRName: DruidRelationName,
                          options: DruidRelationOptions): DruidClusterInfo = {
    cache.synchronized {
      if (cache.contains(dRName.druidHost)) {
        cache(dRName.druidHost)
      } else {
        val host = dRName.druidHost
        val cc = curatorConnection(host, options)
        val svr = cc.getCoordinator
        val dc = new DruidCoordinatorClient(svr)
        val ss = dc.serverStatus
        val r = dc.serversInfo.filter(_.`type` == "historical")
        val dCI = new DruidClusterInfo(host, cc, ss, MMap[String, DruidDataSourceInfo](), r)
        cache(dCI.host) = dCI
        dCI
      }
    }
  }

  def getDataSourceInfo(dRName: DruidRelationName,
                        options: DruidRelationOptions): DruidDataSourceInfo = {
    val dCI = getDruidClusterInfo(dRName, options)
    dCI.synchronized {
      if (dCI.druidDataSources.contains(dRName.druidDataSource)) {
        dCI.druidDataSources(dRName.druidDataSource)
      } else {
        val dc = new DruidCoordinatorClient(dCI.curatorConnection.getCoordinator)
        val r = dc.dataSourceInfo(dRName.druidDataSource)
        val bC = new DruidQueryServerClient(dCI.curatorConnection.getBroker)
        var dds: DruidDataSource = bC.metadata(dRName.druidDataSource,
          options.loadMetadataFromAllSegments)
        dds = dds.copy(druidVersion = dCI.coordinatorStatus.version)
        val i: DruidDataSourceInfo = (r, dds)
        val m = dCI.druidDataSources.asInstanceOf[MMap[String, DruidDataSourceInfo]]
        m(i._1.name) = i
        i
      }
    }
  }

  def hostKey(coordinator: String,
              port: Int) = s"$coordinator:$port"

  def assignHistoricalServers(dRName: DruidRelationName,
                              options: DruidRelationOptions,
                              intervals: List[Interval]): List[HistoricalServerAssignment] = {
    val cInfo = cache.synchronized {
      getDataSourceInfo(dRName, options)
      getDruidClusterInfo(dRName, options).copy()
    }
    cInfo.historicalServers(dRName, intervals)
  }

  def register(dRName: DruidRelationName,
               options: DruidRelationOptions): Unit = {
    getDataSourceInfo(dRName, options)
  }

  def clearCache: Unit = cache.synchronized(cache.clear())

  def clearCache(host: String): Unit = cache.synchronized {
    cache.remove(host)
  }

}