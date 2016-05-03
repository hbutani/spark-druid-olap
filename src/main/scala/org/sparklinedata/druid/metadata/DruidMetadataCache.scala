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
import org.sparklinedata.druid.client.{CuratorConnection, DruidCoordinatorClient}

case class DruidNode(name : String,
                     id : String,
                     address : String,
                     port : Int)

case class ShardSpec(
                      `type` : String,
                      partitionNum : Option[Int],
                      partitions : Option[Int]
)

case class DruidSegmentInfo(dataSource : String,
                            interval : String,
                            version : String,
                            binaryVersion : String,
                            size : Long,
                            identifier : String,
                            shardSpec : Option[ShardSpec]
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
                            curatorConnection : CuratorConnection,
                            dataSources : scala.collection.Map[String,
                              (DataSourceSegmentInfo, DruidDataSource)],
                            histServers : List[HistoricalServerInfo]) {

  def historicalServers(datasource : String, ins : List[Interval]) :
  List[HistoricalServerAssignment] = {

    val m : MMap[String, (HistoricalServerInfo, List[Interval])] = MMap()

    /**
      * - favor the higher priority server
      * - when the priorities are equal
      *   - favor the server with least work.
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

    for(in <- ins) {
      dataSources(datasource)._1.segmentsToScan(in).map { seg =>
        val segIn = seg._interval.overlap(in)
        val s = histServers.filter(_.handlesSegment(seg.identifier)).sorted.head
        if (m.contains(s.host)) {
          val v = m(s.host)
          m(s.host) = (v._1, segIn :: v._2)
        } else {
          m(s.host) = (s, List(segIn))
        }
      }
    }

    m.map {
      case (h, t) => HistoricalServerAssignment(t._1,t._2)
    }.toList
  }

}

trait DruidMetadataCache {
  type DruidDataSourceInfo = (DataSourceSegmentInfo, DruidDataSource)

  def getDruidClusterInfo(host : String,
                          options : DruidRelationOptions) : DruidClusterInfo

  def getDataSourceInfo(host : String,
                        datasource : String,
                        options : DruidRelationOptions) : DruidDataSourceInfo

  def assignHistoricalServers(host : String,
                              dataSourceName : String,
                              options : DruidRelationOptions,
                              intervals : List[Interval]) : List[HistoricalServerAssignment]

  def register(host : String,
               dataSource : String,
               options : DruidRelationOptions) : Unit

  def clearCache(host : String) : Unit
}

trait DruidRelationInfoCache {

  self : DruidMetadataCache =>

  type DruidDataSourceKey = (String, String)

  private[metadata] val druidRelationInfoMap : MMap[DruidDataSourceKey, DruidRelationInfo] = MMap()

  // scalastyle:off parameter.number
  private def _druidRelation(sqlContext : SQLContext,
                             sourceDFName : String,
                             sourceDF : DataFrame,
                             dsName : String,
                             timeDimensionCol : String,
                             druidHost : String,
                             columnMapping : Map[String, String],
                             functionalDeps : List[FunctionalDependency],
                             starSchema : StarSchema,
                             options : DruidRelationOptions) : DruidRelationInfo = {

    val druidDS = getDataSourceInfo(druidHost, dsName, options)._2
    val sourceToDruidMapping =
      MappingBuilder.buildMapping(sqlContext, sourceDFName,
        starSchema, columnMapping, timeDimensionCol, druidDS)
    val fd = new FunctionalDependencies(druidDS, functionalDeps,
      DependencyGraph(druidDS, functionalDeps))

    val dr = DruidRelationInfo(druidHost,
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
                    columnMapping : Map[String, String],
                    functionalDeps : List[FunctionalDependency],
                    starSchema : StarSchema,
                    options : DruidRelationOptions) : DruidRelationInfo =
    druidRelationInfoMap.synchronized {
      druidRelationInfoMap.getOrElse((druidHost, sourceDFName),
        _druidRelation(sqlContext, sourceDFName, sourceDF, dsName, timeDimensionCol,
          druidHost, columnMapping, functionalDeps, starSchema, options)
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

  private def clusterInfoFuture(host : String,
                                options : DruidRelationOptions) : Future[DruidClusterInfo] =
    clusterInfoFutures.synchronized {
    if ( !clusterInfoFutures.contains(host)) {
      clusterInfoFutures(host) = Future {
        val cc = new CuratorConnection(host, options, this, this.thrdPool)
        val svr = cc.getCoordinator
        val dc = new DruidCoordinatorClient(svr)
        val r = dc.serversInfo.filter(_.`type` == "historical")
        new DruidClusterInfo(host, cc, MMap[String, DruidDataSourceInfo](), r)
      }
    }
    clusterInfoFutures(host)
  }

    private def dataSourceInfoFuture(dCI : DruidClusterInfo,
                                   histServer : HistoricalServerInfo,
                                   datasource : String,
                                     options : DruidRelationOptions) :
  Future[DruidDataSourceInfo] =
    clusterInfoFutures.synchronized {
      if ( !dataSourceInfoFutures.contains(datasource)) {
        dataSourceInfoFutures(datasource) = Future {
          val dc = new DruidCoordinatorClient(dCI.curatorConnection.getCoordinator)
          val r = dc.dataSourceInfo(datasource)
          val dds : DruidDataSource = dc.metadataFromHistorical(histServer, datasource, false)
          (r, dds)
        }
      }
      dataSourceInfoFutures(datasource)
    }

  private def _get(host : String,
                   options : DruidRelationOptions) :
  Either[DruidClusterInfo, Future[DruidClusterInfo]] =
    cache.synchronized {
    if ( cache.contains(host)) {
      Left(cache(host))
    } else {
      Right(clusterInfoFuture(host, options))
    }
  }

  private def _put(dCI : DruidClusterInfo) : Unit = cache.synchronized {
    clusterInfoFutures.remove(dCI.host)
    cache(dCI.host) = dCI
  }

  private def getDruidClusterInfoFailure(hostKey : String,
                          options : DruidRelationOptions,
                                         datasource : String,
                          e : Throwable) : DruidClusterInfo = cache.synchronized {
    clusterInfoFutures.remove(hostKey)
    e match {
      case e : DruidDataSourceException => throw e
      case t : Throwable =>
        throw new DruidDataSourceException("failed in future invocation", t)
    }
  }

  private def getDataSourceInfoFailure(hostKey : String,
                                         options : DruidRelationOptions,
                                       datasource : String,
                                         e : Throwable) : DruidDataSourceInfo = cache.synchronized {
    dataSourceInfoFutures.remove(datasource)
    e match {
      case e : DruidDataSourceException => throw e
      case t : Throwable =>
        throw new DruidDataSourceException("failed in future invocation", t)
    }
  }

  private def _add(dCI : DruidClusterInfo, i : DruidDataSourceInfo)
  : Unit = dCI.synchronized {
    val m = dCI.dataSources.asInstanceOf[MMap[String, DruidDataSourceInfo]]
    m(i._1.name) = i
  }

  private def _get(host : String,
                   dataSource : String,
                   options : DruidRelationOptions) :
  Either[DruidDataSourceInfo, Future[DruidDataSourceInfo]] = {
    _get(host, options) match {
      case Left(dCI) => dCI.synchronized {
        if ( dCI.dataSources.contains(dataSource)) {
          Left(dCI.dataSources(dataSource))
        } else {
          Right(dataSourceInfoFuture(dCI, dCI.histServers.head, dataSource, options))
        }
      }
      case Right(f) => Right(f.flatMap{ dCI =>
        _put(dCI)
        dataSourceInfoFuture(dCI, dCI.histServers.head, dataSource, options)
      })
    }
  }

  private def awaitInfo[T](hostKey : String,
                           options : DruidRelationOptions,
                           dataSource : String,
                            f : Future[T],
                           successAction : T => Unit,
                           failureAction :
                           (String, DruidRelationOptions, String, Throwable) => T) : T = {
    Await.ready(f, Duration.Inf)
    cache.synchronized {
      f.value match {
        case Some(Success(i)) => {
          successAction(i)
          i
        }
        case Some(Failure(e)) =>
          failureAction(hostKey, options, dataSource, e)
        case None =>
          throw new DruidDataSourceException("internal error: waiting for future didn't happen")
      }
    }
  }

  def getDruidClusterInfo(hostKey : String,
                          options : DruidRelationOptions) : DruidClusterInfo = {
    _get(hostKey, options) match {
      case Left(i) => i
      case Right(f) => awaitInfo[DruidClusterInfo](hostKey, options, null, f, _put(_),
        getDruidClusterInfoFailure)
    }
  }

  def getDataSourceInfo(hostKey : String,
                        datasource : String,
                        options : DruidRelationOptions) : DruidDataSourceInfo = {
    _get(hostKey, datasource, options) match {
      case Left(i) => i
      case Right(f) =>
        awaitInfo[DruidDataSourceInfo](hostKey, options, datasource, f,
          _add(cache(hostKey), _), getDataSourceInfoFailure)
    }
  }

  def hostKey(coordinator : String,
              port : Int) = s"$coordinator:$port"

  def assignHistoricalServers(coordinator : String,
                              dataSourceName : String,
                              options : DruidRelationOptions,
                              intervals : List[Interval]) : List[HistoricalServerAssignment] = {
    getDruidClusterInfo(coordinator, options
    ).historicalServers(dataSourceName, intervals)

  }

  def register(host : String,
               dataSource : String,
               options : DruidRelationOptions) : Unit = {
    _get(host, dataSource, options)
  }

  def clearCache(host : String) : Unit = druidRelationInfoMap.synchronized {
    druidRelationInfoMap.clear()
    cache.synchronized(cache.clear())
  }


}