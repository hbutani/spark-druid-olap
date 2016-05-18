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
                            druidDataSources : scala.collection.Map[String,
                              (DataSourceSegmentInfo, DruidDataSource)],
                            histServers : List[HistoricalServerInfo]) {

  def historicalServers(dRName : DruidRelationName, ins : List[Interval]) :
  List[HistoricalServerAssignment] = {

    val m : MMap[String, (HistoricalServerInfo, List[(DruidSegmentInfo, Interval)])] = MMap()

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
      druidDataSources(dRName.druidDataSource)._1.segmentsToScan(in).map { seg =>
        val segIn = seg._interval.overlap(in)
        val s = histServers.filter(_.handlesSegment(seg.identifier)).sorted.head
        if (m.contains(s.host)) {
          val v = m(s.host)
          val segInTuple  = (seg, segIn)
          m(s.host) = (v._1, segInTuple :: v._2)
        } else {
          m(s.host) = (s, List((seg,segIn)))
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

  def getDruidClusterInfo(druidRelName : DruidRelationName,
                          options : DruidRelationOptions) : DruidClusterInfo

  def getDataSourceInfo(druidRelName : DruidRelationName,
                        options : DruidRelationOptions) : DruidDataSourceInfo

  def assignHistoricalServers(druidRelName : DruidRelationName,
                              options : DruidRelationOptions,
                              intervals : List[Interval]) : List[HistoricalServerAssignment]

  def register(druidRelName : DruidRelationName,
               options : DruidRelationOptions) : Unit

  def clearCache(host : String) : Unit
}

trait DruidRelationInfoCache {

  self : DruidMetadataCache =>

  type DruidDataSourceKey = (String, String)

  private[metadata] val druidRelationInfoMap : MMap[DruidRelationName, DruidRelationInfo] = MMap()

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
    druidRelationInfoMap(fullName) = dr
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
      val fullName = DruidRelationName(starSchema.factTable.name, druidHost, dsName)
      druidRelationInfoMap.getOrElse(fullName,
        _druidRelation(sqlContext, sourceDFName, sourceDF, dsName, timeDimensionCol,
          druidHost, columnMapping, functionalDeps, starSchema, options)
      )
    }

}

case class HistoricalServerAssignment(server : HistoricalServerInfo,
                                      segmentIntervals : List[(DruidSegmentInfo, Interval)])

object DruidMetadataCache extends DruidMetadataCache  with DruidRelationInfoCache {

  private[metadata] val cache : MMap[String, DruidClusterInfo] = MMap()
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
                                     dRName : DruidRelationName,
                                     options : DruidRelationOptions) :
  Future[DruidDataSourceInfo] =
    clusterInfoFutures.synchronized {
      if ( !dataSourceInfoFutures.contains(dRName.druidDataSource)) {
        dataSourceInfoFutures(dRName.druidDataSource) = Future {
          val dc = new DruidCoordinatorClient(dCI.curatorConnection.getCoordinator)
          val r = dc.dataSourceInfo(dRName.druidDataSource)
          val bC = new DruidQueryServerClient(dCI.curatorConnection.getBroker)
          val dds : DruidDataSource = bC.metadata(dRName.druidDataSource,
            options.loadMetadataFromAllSegments)
          (r, dds)
        }
      }
      dataSourceInfoFutures(dRName.druidDataSource)
    }

  private def _getClusterInfo(dRName : DruidRelationName,
                   options : DruidRelationOptions) :
  Either[DruidClusterInfo, Future[DruidClusterInfo]] =
    cache.synchronized {
    if ( cache.contains(dRName.druidHost)) {
      Left(cache(dRName.druidHost))
    } else {
      Right(clusterInfoFuture(dRName.druidHost, options))
    }
  }

  private def _put(dCI : DruidClusterInfo) : Unit = cache.synchronized {
    clusterInfoFutures.remove(dCI.host)
    cache(dCI.host) = dCI
  }

  private def handleClusterInfoFailure(dRName : DruidRelationName,
                          options : DruidRelationOptions,
                          e : Throwable) : DruidClusterInfo = cache.synchronized {
    clusterInfoFutures.remove(dRName.druidHost)
    e match {
      case e : DruidDataSourceException => throw e
      case t : Throwable =>
        throw new DruidDataSourceException("failed in future invocation", t)
    }
  }

  private def handleDataSourceFailure(dRName : DruidRelationName,
                                         options : DruidRelationOptions,
                                         e : Throwable) : DruidDataSourceInfo = cache.synchronized {
    dataSourceInfoFutures.remove(dRName.druidDataSource)
    e match {
      case e : DruidDataSourceException => throw e
      case t : Throwable =>
        throw new DruidDataSourceException("failed in future invocation", t)
    }
  }

  private def _add(dCI : DruidClusterInfo, i : DruidDataSourceInfo)
  : Unit = dCI.synchronized {
    val m = dCI.druidDataSources.asInstanceOf[MMap[String, DruidDataSourceInfo]]
    m(i._1.name) = i
  }

  private def _getDataSourceInfo(dRName : DruidRelationName,
                   options : DruidRelationOptions) :
  Either[DruidDataSourceInfo, Future[DruidDataSourceInfo]] = {
    _getClusterInfo(dRName, options) match {
      case Left(dCI) => dCI.synchronized {
        if ( dCI.druidDataSources.contains(dRName.druidDataSource)) {
          Left(dCI.druidDataSources(dRName.druidDataSource))
        } else {
          Right(dataSourceInfoFuture(dCI, dCI.histServers.head, dRName, options))
        }
      }
      case Right(f) => Right(f.flatMap{ dCI =>
        _put(dCI)
        dataSourceInfoFuture(dCI, dCI.histServers.head, dRName, options)
      })
    }
  }

  private def awaitInfo[T](dRName : DruidRelationName,
                           options : DruidRelationOptions,
                            f : Future[T],
                           successAction : T => Unit,
                           failureAction :
                           (DruidRelationName, DruidRelationOptions, Throwable) => T) : T = {
    Await.ready(f, Duration.Inf)
    cache.synchronized {
      f.value match {
        case Some(Success(i)) => {
          successAction(i)
          i
        }
        case Some(Failure(e)) =>
          failureAction(dRName, options, e)
        case None =>
          throw new DruidDataSourceException("internal error: waiting for future didn't happen")
      }
    }
  }

  def getDruidClusterInfo(dRName : DruidRelationName,
                          options : DruidRelationOptions) : DruidClusterInfo = {
    _getClusterInfo(dRName, options) match {
      case Left(i) => i
      case Right(f) => awaitInfo[DruidClusterInfo](dRName, options, f, _put(_),
        handleClusterInfoFailure)
    }
  }

  def getDataSourceInfo(dRName : DruidRelationName,
                        options : DruidRelationOptions) : DruidDataSourceInfo = {
    _getDataSourceInfo(dRName, options) match {
      case Left(i) => i
      case Right(f) =>
        awaitInfo[DruidDataSourceInfo](dRName, options, f,
          _add(cache(dRName.druidHost), _), handleDataSourceFailure)
    }
  }

  def hostKey(coordinator : String,
              port : Int) = s"$coordinator:$port"

  def assignHistoricalServers(dRName : DruidRelationName,
                              options : DruidRelationOptions,
                              intervals : List[Interval]) : List[HistoricalServerAssignment] = {
    getDataSourceInfo(dRName, options)
    getDruidClusterInfo(dRName, options
    ).historicalServers(dRName, intervals)

  }

  def register(dRName : DruidRelationName,
               options : DruidRelationOptions) : Unit = {
    _getDataSourceInfo(dRName, options)
  }

  def clearCache : Unit = druidRelationInfoMap.synchronized {
    druidRelationInfoMap.clear()
    cache.synchronized(cache.clear())
  }

  def clearCache(host : String) : Unit = clearCache


}