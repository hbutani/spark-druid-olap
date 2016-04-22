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
import org.sparklinedata.druid.client.DruidCoordinatorClient

case class DruidNode(name : String,
                     id : String,
                     address : String,
                     port : Int)

case class DruidSegmentInfo(dataSource : String,
                            interval : String,
                            version : String,
                            binaryVersion : String,
                            size : Long,
                            identifier : String
                           ) {

  lazy val _interval = Interval.parse(interval)

  def overlaps(in : Interval) = _interval.overlaps(in)
}

case class HistoricalServerInfo(host : String,
                                maxSize : Long,
                                `type` : String,
                                tier : String,
                                priority : Int,
                                segments : Map[String, DruidSegmentInfo],
                                currSize : Long) {

  def handlesSegment(segId : String) = segments.contains(segId)
}

case class DataSourceSegmentInfo(name : String,
                                 properties : Map[String, String],
                                 segments : List[DruidSegmentInfo]
                         ) {

  def segmentsToScan(in : Interval) : List[DruidSegmentInfo] = segments.filter(_.overlaps(in))

}

case class DruidClusterInfo(host : String,
                            dataSources : scala.collection.Map[String,
                              (DataSourceSegmentInfo, DruidDataSource)],
                            histServers : List[HistoricalServerInfo]) {

  def historicalServers(datasource : String, ins : List[Interval]) :
  List[HistoricalServerAssignment] = {

    val m : MMap[HistoricalServerInfo, List[Interval]] = MMap()

    /**
      * - favor the higher priority server
      * - when the priorities are equal
      *   - favor the server with least work.
      */
    implicit val o = new Ordering[HistoricalServerInfo] {
      override def compare(x: HistoricalServerInfo, y: HistoricalServerInfo): Int = {
        if (x.priority == y.priority) {
          (m.get(x), m.get(y)) match {
            case (None, None) => 0 // x
            case (None, _) => -1 // x
            case (_, None) => 1 // y
            case (Some(l1), Some(l2)) => l1.size - l2.size
          }

        } else {
          y.priority - x.priority
        }
      }
    }

    for(in <- ins) {
      dataSources(datasource)._1.segmentsToScan(in).map { seg =>
        val s = histServers.filter(_.handlesSegment(seg.identifier)).sorted.head
        if (m.contains(s)) {
          m(s) = seg._interval :: m(s)
        } else {
          m(s) = List(seg._interval)
        }
      }
    }

    m.map {
      case (h, l) => HistoricalServerAssignment(h,l)
    }.toList
  }

}

trait DruidMetadataCache {
  type DruidDataSourceInfo = (DataSourceSegmentInfo, DruidDataSource)

  def getDruidClusterInfo(coordinator : String,
                          port : Int) : DruidClusterInfo

  def getDataSourceInfo(coordinator : String,
                        port : Int,
                        datasource : String) : DruidDataSourceInfo

  def assignHistoricalServers(coordinator : String,
                              port : Int,
                              dataSourceName : String,
                              intervals : List[Interval]) : List[HistoricalServerAssignment]

  def register(coordinator : String, port : Int, dataSource : String) : Unit

  def clearCache(host : String) : Unit
}

trait DruidRelationInfoCache {

  self : DruidMetadataCache =>

  type DruidDataSourceKey = (String, String)

  private val druidRelationInfoMap : MMap[DruidDataSourceKey, DruidRelationInfo] = MMap()

  // scalastyle:off parameter.number
  private def _druidRelation(sqlContext : SQLContext,
                             sourceDFName : String,
                             sourceDF : DataFrame,
                             dsName : String,
                             timeDimensionCol : String,
                             druidHost : String,
                             druidPort : Int,
                             columnMapping : Map[String, String],
                             functionalDeps : List[FunctionalDependency],
                             starSchema : StarSchema,
                             options : DruidRelationOptions) : DruidRelationInfo = {

    val druidDS = getDataSourceInfo(druidHost, druidPort, dsName)._2
    val sourceToDruidMapping =
      MappingBuilder.buildMapping(sqlContext, sourceDFName,
        starSchema, columnMapping, timeDimensionCol, druidDS)
    val fd = new FunctionalDependencies(druidDS, functionalDeps,
      DependencyGraph(druidDS, functionalDeps))

    val dr = DruidRelationInfo(DruidClientInfo(druidHost, druidPort),
      sourceDFName,
      timeDimensionCol,
      druidDS,
      sourceToDruidMapping,
      fd,
      starSchema,
      options)
    druidRelationInfoMap((druidHost, sourceDFName)) = dr
    dr
  }

  def druidRelation(sqlContext : SQLContext,
                    sourceDFName : String,
                    sourceDF : DataFrame,
                    dsName : String,
                    timeDimensionCol : String,
                    druidHost : String,
                    druidPort : Int,
                    columnMapping : Map[String, String],
                    functionalDeps : List[FunctionalDependency],
                    starSchema : StarSchema,
                    options : DruidRelationOptions) : DruidRelationInfo =
    druidRelationInfoMap.synchronized {
      druidRelationInfoMap.getOrElse((druidHost, sourceDFName),
        _druidRelation(sqlContext, sourceDFName, sourceDF, dsName, timeDimensionCol,
          druidHost, druidPort, columnMapping, functionalDeps, starSchema, options)
      )
    }

}

case class HistoricalServerAssignment(server : HistoricalServerInfo,
                                      intervals : List[Interval])

object DruidMetadataCache extends DruidMetadataCache  with DruidRelationInfoCache {

  private val cache : MMap[String, DruidClusterInfo] = MMap()
  private val clusterInfoFutures : MMap[String, Future[DruidClusterInfo]] = MMap()
  private val dataSourceInfoFutures : MMap[String, Future[DruidDataSourceInfo]] = MMap()

  val thrdPool = SparklineThreadUtils.newDaemonCachedThreadPool("druidMD", 5)
  implicit val ec = ExecutionContext.fromExecutor(thrdPool)

  private def clusterInfoFuture(host : String) : Future[DruidClusterInfo] =
    clusterInfoFutures.synchronized {
    if ( !clusterInfoFutures.contains(host)) {
      clusterInfoFutures(host) = Future {
        val dc = new DruidCoordinatorClient(host)
        val r = dc.serversInfo
        new DruidClusterInfo(host, MMap[String, DruidDataSourceInfo](), r)
      }
    }
    clusterInfoFutures(host)
  }

    private def dataSourceInfoFuture(host : String,
                                   histServer : String,
                                   datasource : String) :
  Future[DruidDataSourceInfo] =
    clusterInfoFutures.synchronized {
      if ( !dataSourceInfoFutures.contains(datasource)) {
        dataSourceInfoFutures(datasource) = Future {
          val dc = new DruidCoordinatorClient(host)
          val r = dc.dataSourceInfo(datasource)
          val dds : DruidDataSource = dc.metadataFromHistorical(histServer, datasource, false)
          (r, dds)
        }
      }
      dataSourceInfoFutures(datasource)
    }

  private def _get(host : String) : Either[DruidClusterInfo, Future[DruidClusterInfo]] =
    cache.synchronized {
    if ( cache.contains(host)) {
      Left(cache(host))
    } else {
      Right(clusterInfoFuture(host))
    }
  }

  private def _put(dCI : DruidClusterInfo) : Unit = cache.synchronized {
    clusterInfoFutures.remove(dCI.host)
    cache(dCI.host) = dCI
  }

  private def _add(dCI : DruidClusterInfo, i : DruidDataSourceInfo)
  : Unit = dCI.synchronized {
    val m = dCI.dataSources.asInstanceOf[MMap[String, DruidDataSourceInfo]]
    m(i._1.name) = i
  }

  private def _get(host : String, dataSource : String) :
  Either[DruidDataSourceInfo, Future[DruidDataSourceInfo]] = {
    _get(host) match {
      case Left(dCI) => dCI.synchronized {
        if ( dCI.dataSources.contains(dataSource)) {
          Left(dCI.dataSources(dataSource))
        } else {
          Right(dataSourceInfoFuture(dCI.host, dCI.histServers.head.host, dataSource))
        }
      }
      case Right(f) => Right(f.flatMap{ dCI =>
        _put(dCI)
        dataSourceInfoFuture(dCI.host, dCI.histServers.head.host, dataSource)
      })
    }
  }

  private def awaitInfo[T](f : Future[T], successAction : T => Unit) : T = {
    Await.ready(f, Duration.Inf)
    cache.synchronized {
      f.value match {
        case Some(Success(i)) => {
          successAction(i)
          i
        }
        case Some(Failure(e)) =>  e match {
          case e : DruidDataSourceException => throw e
          case t : Throwable =>
            throw new DruidDataSourceException("failed in future invocation", t)
        }
        case None =>
          throw new DruidDataSourceException("internal error: waiting for future didn't happen")
      }
    }
  }

  def getDruidClusterInfo(hostKey : String) : DruidClusterInfo = {
    _get(hostKey) match {
      case Left(i) => i
      case Right(f) => awaitInfo[DruidClusterInfo](f, _put(_))
    }
  }

  def getDruidClusterInfo(coordinator : String,
                          port : Int) : DruidClusterInfo =
    getDruidClusterInfo(hostKey(coordinator, port))

  def getDataSourceInfo(hostKey : String, datasource : String) : DruidDataSourceInfo = {
    _get(hostKey, datasource) match {
      case Left(i) => i
      case Right(f) =>
        awaitInfo[DruidDataSourceInfo](f, _add(cache(hostKey), _))
    }
  }

  def getDataSourceInfo(coordinator : String,
                        port : Int,
                        datasource : String) : DruidDataSourceInfo =
    getDataSourceInfo(hostKey(coordinator, port), datasource)

  def hostKey(coordinator : String,
              port : Int) = s"$coordinator:$port"

  def assignHistoricalServers(coordinator : String,
                              port : Int,
                              dataSourceName : String,
                              intervals : List[Interval]) : List[HistoricalServerAssignment] = {
    getDruidClusterInfo(hostKey(coordinator, port)).historicalServers(dataSourceName, intervals)

  }

  def register(coordinator : String, port : Int, dataSource : String) : Unit = {
    _get(hostKey(coordinator, port), dataSource)
  }

  def clearCache(host : String) : Unit = cache.synchronized(cache.clear())


}