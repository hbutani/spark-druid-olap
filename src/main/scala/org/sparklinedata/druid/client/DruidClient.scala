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

package org.sparklinedata.druid.client

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.client.methods._
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.druid.DruidQueryResultIterator
import org.joda.time.{DateTime, Interval}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.{DruidDataSourceException, QuerySpec}
import org.sparklinedata.druid.metadata.{DependencyGraph, DruidClientInfo, DruidRelationInfo, _}
import org.sparklinedata.druid.Utils
import org.sparklinedata.druid.CloseableIterator

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

object ConnectionManager {
  val pool = new PoolingHttpClientConnectionManager
}

abstract class DruidClient(val host : String,
                  val port : Int) extends Logging {

  import Utils._

  def this(t : (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s : String) = {
    this(DruidClient.hosPort(s))
  }

  protected def httpClient: CloseableHttpClient = {
    HttpClients.custom.setConnectionManager(ConnectionManager.pool).build
  }

  protected def release(resp: CloseableHttpResponse) : Unit = {
    Try {
      if (resp != null) EntityUtils.consume(resp.getEntity)
    } recover {
      case ex => log.error("Error returning client to pool", ExceptionUtils.getStackTrace(ex))
    }
  }

  protected def getRequest(url : String) = new HttpGet(url)
  protected def postRequest(url : String) = new HttpPost(url)

  @throws(classOf[DruidDataSourceException])
  protected def perform(url : String,
                       reqType : String => HttpRequestBase,
                      payload : String,
                      reqHeaders: Map[String, String]) : JValue =  {
    var resp: CloseableHttpResponse = null

    val js : Try[JValue] = for {
      r <- Try {
        val req: CloseableHttpClient = httpClient
        val request: HttpRequestBase = reqType(url)
        if ( payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          val input: StringEntity = new StringEntity(payload, ContentType.APPLICATION_JSON)
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        resp = req.execute(request)
        resp
      }
      respStr <- Try {
        val status = r.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
          IOUtils.toString(r.getEntity.getContent)
        } else {
          throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
        }
      }
      j <- Try {parse(respStr)}
    } yield j

    release(resp)
    js getOrElse( js.failed.get match {
      case dE : DruidDataSourceException => throw dE
      case x => throw new DruidDataSourceException("Failed in communication with Druid", x)
    })
  }

  @throws(classOf[DruidDataSourceException])
  protected def performQuery(url : String,
                            reqType : String => HttpRequestBase,
                           payload : String,
                           reqHeaders: Map[String, String]) : CloseableIterator[QueryResultRow] =  {
    var resp: CloseableHttpResponse = null

    try {
      val req: CloseableHttpClient = httpClient
      val request: HttpRequestBase = reqType(url)
      if (payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
        val input: StringEntity = new StringEntity(payload, ContentType.APPLICATION_JSON)
        request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
      }
      addHeaders(request, reqHeaders)
      resp = req.execute(request)
      val status = resp.getStatusLine().getStatusCode();
      if (status >= 200 && status < 300) {
        DruidQueryResultIterator(resp.getEntity.getContent, {release(resp)})
      } else {
        throw new DruidDataSourceException(s"Unexpected response status: ${resp.getStatusLine}")
      }
    } catch {
      case dE: DruidDataSourceException => throw dE
      case x : Throwable =>
        throw new DruidDataSourceException("Failed in communication with Druid", x)
    }
  }

  protected def post(url : String,
                     payload : String,
                   reqHeaders: Map[String, String] = null ) : JValue =
    perform(url, postRequest _, payload, reqHeaders)

  protected def postQuery(url : String,
                          payload : String,
                        reqHeaders: Map[String, String] = null ) :
  CloseableIterator[QueryResultRow] =
    performQuery(url, postRequest _, payload, reqHeaders)

  protected def get(url : String,
                    payload : String = null,
                  reqHeaders: Map[String, String] = null) : JValue =
    perform(url, getRequest _, payload, reqHeaders)

  protected def addHeaders(req: HttpRequestBase, reqHeaders: Map[String, String]) {
    if (reqHeaders == null) return
    for (key <- reqHeaders.keySet) {
      req.setHeader(key, reqHeaders(key))
    }
  }

  @throws(classOf[DruidDataSourceException])
  def executeQuery(url : String,
                   qry : QuerySpec) : List[QueryResultRow] = {

    val jR = compact(render(Extraction.decompose(qry)))
    val jV = post(url, jR)

    jV.extract[List[QueryResultRow]]

  }

  @throws(classOf[DruidDataSourceException])
  def executeQueryAsStream(url : String,
                           qry : QuerySpec) : CloseableIterator[QueryResultRow] = {
    val jR = compact(render(Extraction.decompose(qry)))
    postQuery(url, jR)
  }

  def timeBoundary(dataSource : String) : Interval

}

object DruidClient {

  val HOST = """([^:]*):(\d*)""".r

  def hosPort(s : String) : (String, Int) = {
    val HOST(h, p) = s
    (h, p.toInt)
  }

}

class DruidBrokerClient(host : String, port : Int)
  extends DruidClient(host, port) with Logging {

  import org.json4s.JsonDSL._
  import Utils._

  @transient val url = s"http://$host:$port/druid/v2/?pretty"

  def this(t : (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s : String) = {
    this(DruidClient.hosPort(s))
  }

  @throws(classOf[DruidDataSourceException])
  def timeBoundary(dataSource : String) : Interval = {

    val jR = compact(render(
      ( "queryType" -> "timeBoundary") ~ ("dataSource" -> dataSource)
    ))
    val jV = post(url, jR)

    val sTime : String = (jV \\ "minTime").extract[String]
    val eTime : String = (jV \\ "maxTime").extract[String]

    val endDate = DateTime.parse(eTime).plusSeconds(1)

    new Interval(DateTime.parse(sTime), endDate)
  }

  @throws(classOf[DruidDataSourceException])
  def metadata(dataSource : String, fullIndex : Boolean) : DruidDataSource = {

    val in = timeBoundary(dataSource)

    val i = if (fullIndex) in.toString else in.withEnd(in.getStart.plusMillis(1)).toString

    val jR = compact(render(
      ("queryType" -> "segmentMetadata") ~ ("dataSource" -> dataSource) ~
        ("intervals" -> List(i)) ~ ("merge" -> "true")
    ))
    val jV = post(url, jR) transformField {
      case ("type", x) => ("typ", x)
    }

    val l = jV.extract[List[MetadataResponse]]
    DruidDataSource(dataSource, l.head, List(in))
  }

  @throws(classOf[DruidDataSourceException])
  def segmentInfo(dataSource : String) : List[SegmentInfo] = {

    val in = timeBoundary(dataSource)

    val t = render(("type" -> "none"))

    val jR = compact(render(
      ("queryType" -> "segmentMetadata") ~ ("dataSource" -> dataSource) ~
        ("intervals" -> List(in.toString)) ~
        ("merge" -> "false") ~
        ("toInclude" -> render(("type" -> "none")))
    ))
    val jV = post(url, jR) transformField {
      case ("type", x) => ("typ", x)
    }

    val l = jV.extract[List[MetadataResponse]]
    l.map(new SegmentInfo(_))
  }

  @throws(classOf[DruidDataSourceException])
  def executeQuery(qry : QuerySpec) : List[QueryResultRow] = {
    executeQuery(url, qry)
  }

  @throws(classOf[DruidDataSourceException])
  def executeQueryAsStream(qry : QuerySpec) : CloseableIterator[QueryResultRow] = {
    executeQueryAsStream(url, qry)
  }
}


object DruidBrokerClient {

  type DruidDataSourceKey = (String, String)

  val druidRelationInfoMap : MMap[DruidDataSourceKey, DruidRelationInfo] = MMap()

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

    val client = new DruidBrokerClient(druidHost, druidPort)
    val druidDS = client.metadata(dsName, options.loadMetadataFromAllSegments)
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


class DruidCoordinatorClient(host : String, port : Int)
  extends DruidClient(host, port) with Logging {

  import org.json4s.JsonDSL._
  import Utils._

  @transient val urlPrefix = s"http://$host:$port/druid/coordinator/v1"

  def this(t: (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s: String) = {
    this(DruidClient.hosPort(s))
  }

  @throws(classOf[DruidDataSourceException])
  def timeBoundary(dataSource : String) : Interval = {

    val url = s"$urlPrefix/datasources/$dataSource"
    val jV = get(url)
    val i = jV.extract[CoordDataSourceInfo]
    new Interval(i.segments.minTime, i.segments.maxTime)
  }

//  @throws(classOf[DruidDataSourceException])
//  def metadata(dataSource : String, fullIndex : Boolean) : DruidDataSource = {
//
//    val in = timeBoundary(dataSource)
//
//    val i = if (fullIndex) in.toString else in.withEnd(in.getStart.plusMillis(1)).toString
//
//    val jR = compact(render(
//      ("queryType" -> "segmentMetadata") ~ ("dataSource" -> dataSource) ~
//        ("intervals" -> List(i)) ~ ("merge" -> "true")
//    ))
//    val jV = post(url, jR) transformField {
//      case ("type", x) => ("typ", x)
//    }
//
//    val l = jV.extract[List[MetadataResponse]]
//    DruidDataSource(dataSource, l.head, List(in))
//  }

  @throws(classOf[DruidDataSourceException])
  def serversInfo : List[HistoricalServerInfo] = {
    val url = s"$urlPrefix/servers?full=true"
    val jV = get(url)
    jV.extract[List[HistoricalServerInfo]]
  }

  @throws(classOf[DruidDataSourceException])
  def dataSourceInfo(datasource : String) : DataSourceInfo = {
    val url = s"$urlPrefix/datasources/$datasource?full=true"
    val jV = get(url)
    jV.extract[DataSourceInfo]
  }
}