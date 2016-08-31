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

import java.io.InputStream

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.HttpEntity
import org.apache.http.client.methods._
import org.apache.http.concurrent.Cancellable
import org.apache.http.entity.{ByteArrayEntity, ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.joda.time.{DateTime, Interval}
import org.json4s._
import org.json4s.jackson.sparklinedata.SmileJsonMethods._
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata._

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

object ConnectionManager {

  @volatile private var  initialized : Boolean = false

  lazy val pool = {
    val p = new PoolingHttpClientConnectionManager
    p.setMaxTotal(40)
    p.setDefaultMaxPerRoute(8)
    p
  }

  def init(sqlContext : SQLContext): Unit = {
    if (!initialized ) {
      init(DruidPlanner.getConfValue(sqlContext,
        DruidPlanner.DRUID_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE),
        DruidPlanner.getConfValue(sqlContext,
          DruidPlanner.DRUID_CONN_POOL_MAX_CONNECTIONS))
      initialized = true
    }
  }

  def init(maxPerRoute : Int, maxTotal : Int): Unit = {
    if (!initialized ) {
      pool.setMaxTotal(maxTotal)
      pool.setDefaultMaxPerRoute(maxPerRoute)
      initialized = true
    }
  }
}

/**
  * A mechanism to relay [[org.apache.http.concurrent.Cancellable]] resources
  * associated with the '''http connection''' of a ''DruidClient''. This is
  * used by the [[org.sparklinedata.druid.TaskCancelHandler]] to capture the
  * association between '''Spark Tasks''' and ''Cancellable'' resources.
  */
trait CancellableHolder {
  def setCancellable(c : Cancellable)
}

/**
  * A mixin trait that relays [[org.apache.http.concurrent.Cancellable]] resources
  * to a [[org.sparklinedata.druid.client.CancellableHolder]]
  */
trait DruidClientHttpExecutionAware extends HttpExecutionAware {

  val ch : CancellableHolder

  abstract override def isAborted = super.isAborted

  abstract override def setCancellable(cancellable: Cancellable) = {
    if ( ch != null ) {
      ch.setCancellable(cancellable)
    }
    super.setCancellable(cancellable)
  }

}

/**
  * Configure HttpPost to have the [[org.sparklinedata.druid.client.DruidClientHttpExecutionAware]]
  * trait, so that [[org.apache.http.concurrent.Cancellable]] resources are relayed to the
  * registered holder.

  */
class DruidHttpPost(url : String,
                    val ch : CancellableHolder)
  extends HttpPost(url) with DruidClientHttpExecutionAware

/**
  * Configure HttpGet to have the [[org.sparklinedata.druid.client.DruidClientHttpExecutionAware]]
  * trait, so that [[org.apache.http.concurrent.Cancellable]] resources are relayed to the
  * registered holder.

  */

class DruidHttpGet(url : String,
                    val ch : CancellableHolder)
  extends HttpGet(url) with DruidClientHttpExecutionAware

/*
 * ''DruidClient'' is not thread-safe. ''cancellableHolder'' state is used to relay
 * cancellable resources information
 */
abstract class DruidClient(val host : String,
                           val port : Int,
                           val useSmile : Boolean = false) extends Logging {

  private var cancellableHolder : CancellableHolder = null

  import Utils._
  import org.json4s.JsonDSL._

  def this(t : (String, Int)) = {
    this(t._1, t._2)
  }

  def this(s : String) = {
    this(DruidClient.hosPort(s))
  }

  def setCancellableHolder(c : CancellableHolder) : Unit = {
    cancellableHolder = c
  }

  protected def httpClient: CloseableHttpClient = {
    val sTime = System.currentTimeMillis()
    val r = HttpClients.custom.setConnectionManager(ConnectionManager.pool).build
    val eTime = System.currentTimeMillis()
    log.debug("Time to get httpClient: {}",  eTime - sTime)
    log.debug(s"Pool Stats: {}",  ConnectionManager.pool.getTotalStats)
    r
  }

  protected def release(resp: CloseableHttpResponse) : Unit = {
    Try {
      if (resp != null) EntityUtils.consume(resp.getEntity)
    } recover {
      case ex => log.error("Error returning client to pool", ExceptionUtils.getStackTrace(ex))
    }
  }

  protected def getRequest(url : String) = new DruidHttpGet(url, cancellableHolder)
  protected def postRequest(url : String) = new DruidHttpPost(url, cancellableHolder)

  @throws(classOf[DruidDataSourceException])
  protected def perform(url : String,
                        reqType : String => HttpRequestBase,
                        payload : JValue,
                        reqHeaders: Map[String, String]) : JValue =  {
    var resp: CloseableHttpResponse = null

    val js : Try[JValue] = for {
      (request, r) <- Try {
        val req: CloseableHttpClient = httpClient
        val request: HttpRequestBase = reqType(url)
        if ( payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          val input: HttpEntity = if (!useSmile) {
            val s : String = compact(payload)
            new StringEntity(s, ContentType.APPLICATION_JSON)
          } else {
            val b = mapper.writeValueAsBytes(payload)
            new ByteArrayEntity(b, null)
          }
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        resp = req.execute(request)
        (request, resp)
      }
      respStr <- Try {
        val status = r.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
          if (r.getEntity != null ) {
            if (request.isInstanceOf[HttpGet] || !useSmile) {
              IOUtils.toString(r.getEntity.getContent)
            } else {
              r.getEntity.getContent
            }
          } else {
            throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
          }
        } else {
          throw new DruidDataSourceException(s"Unexpected response status: ${r.getStatusLine}")
        }
      }
      j <- Try {
        respStr match {
          case rs: InputStream => parse(rs)
          case rs => parse(rs.toString)
        }
      }
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
                             qrySpec : QuerySpec,
                             payload : JValue,
                             reqHeaders: Map[String, String]) : CloseableIterator[ResultRow] =  {
    var resp: CloseableHttpResponse = null

    val enterTime = System.currentTimeMillis()
    var beforeExecTime : Long = System.currentTimeMillis()
    var afterExecTime : Long = System.currentTimeMillis()

    val it: Try[CloseableIterator[ResultRow]] = for {
      r <- Try {
        val req: CloseableHttpClient = httpClient
        val request: HttpRequestBase = reqType(url)
        if (payload != null && request.isInstanceOf[HttpEntityEnclosingRequestBase]) {
          val input: HttpEntity = if (!useSmile) {
            val s: String = compact(payload)
            new StringEntity(s, ContentType.APPLICATION_JSON)
          } else {
            val b = mapper.writeValueAsBytes(payload)
            new ByteArrayEntity(b, null)
          }
          request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
        }
        addHeaders(request, reqHeaders)
        beforeExecTime = System.currentTimeMillis()
        resp = req.execute(request)
        afterExecTime = System.currentTimeMillis()
        resp
      }
      it <- Try {
        val status = r.getStatusLine().getStatusCode()
        if (status >= 200 && status < 300) {
          qrySpec(useSmile, r.getEntity.getContent, this, {
            release(r)
          })
        } else {
          throw new DruidDataSourceException(s"Unexpected response status: ${resp.getStatusLine} " +
            s"on $url for query: \n $payload")
        }
      }
    } yield it
    val afterItrBuildTime = System.currentTimeMillis()
    log.debug("{}: beforeExecTime={}, execTime={}, itrBuildTime={}",
      url,
      (beforeExecTime - enterTime).toString,
      (afterExecTime - beforeExecTime).toString,
      (afterItrBuildTime - afterExecTime).toString
    )
    it.getOrElse {
      release(resp)
      it.failed.get match {
        case dE: DruidDataSourceException => throw dE
        case x => throw new DruidDataSourceException("Failed in communication with Druid", x)
      }
    }
  }

  protected def post(url : String,
                     payload : JValue,
                     reqHeaders: Map[String, String] = null ) : JValue =
    perform(url, postRequest _, payload, reqHeaders)

  def postQuery(url : String,
                          qrySpec : QuerySpec,
                          payload : JValue,
                          reqHeaders: Map[String, String] = null ) :
  CloseableIterator[ResultRow] =
    performQuery(url, postRequest _, qrySpec, payload, reqHeaders)

  protected def get(url : String,
                    payload : JValue = null,
                    reqHeaders: Map[String, String] = null) : JValue =
    perform(url, getRequest _, payload, reqHeaders)

  protected def addHeaders(req: HttpRequestBase, reqHeaders: Map[String, String]) {

    if (useSmile) {
      req.addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
    }

    if (reqHeaders == null) return
    for (key <- reqHeaders.keySet) {
      req.setHeader(key, reqHeaders(key))
    }
  }

  @throws(classOf[DruidDataSourceException])
  def executeQuery(url : String,
                   qry : QuerySpec) : List[ResultRow] = {

    val jR = render(Extraction.decompose(qry))
    val jV = post(url, jR)

    jV.extract[List[QueryResultRow]]

  }

  @throws(classOf[DruidDataSourceException])
  def executeQueryAsStream(url : String,
                           qry : QuerySpec) : CloseableIterator[ResultRow] = {
    val jR = render(Extraction.decompose(qry))
    postQuery(url, qry, jR)
  }

  def timeBoundary(dataSource : String) : Interval

  @throws(classOf[DruidDataSourceException])
  def metadata(url : String,
               dataSource : String,
               segs : List[DruidSegmentInfo],
               fullIndex : Boolean) : DruidDataSource = {

    val in = timeBoundary(dataSource)
    val ins : JValue = if ( segs != null ) {
      Extraction.decompose(SegmentIntervals.segmentIntervals(segs))
    } else {
      val i = if (fullIndex) in.toString else in.withEnd(in.getStart.plusMillis(1)).toString
      List(i)
    }

    val jR = render(
      ("queryType" -> "segmentMetadata") ~ ("dataSource" -> dataSource) ~
        ("intervals" -> ins) ~
        ("analysisTypes" -> List[String]("cardinality")) ~
        ("merge" -> "true")
    )

    val jV = post(url, jR) transformField {
      case ("type", x) => ("typ", x)
    }
    val l = jV.extract[List[MetadataResponse]]
    DruidDataSource(dataSource, l.head, List(in))
  }

  def serverStatus : ServerStatus = {
    val url = s"http://$host:$port/status"
    val jV = get(url)
    jV.extract[ServerStatus]
  }

}

object DruidClient {

  val HOST = """([^:]*):(\d*)""".r

  def hosPort(s : String) : (String, Int) = {
    val HOST(h, p) = s
    (h, p.toInt)
  }

}

class DruidQueryServerClient(host : String, port : Int, useSmile : Boolean = false)
  extends DruidClient(host, port, useSmile) with Logging {

  import Utils._
  import org.json4s.JsonDSL._

  @transient val url = s"http://$host:$port/druid/v2/?pretty"

  def this(t : (String, Int), useSmile : Boolean) = {
    this(t._1, t._2, useSmile)
  }

  def this(s : String, useSmile : Boolean = false) = {
    this(DruidClient.hosPort(s), useSmile)
  }

  @throws(classOf[DruidDataSourceException])
  def timeBoundary(dataSource : String) : Interval = {

    val jR = render(
      ( "queryType" -> "timeBoundary") ~ ("dataSource" -> dataSource)
    )
    val jV = post(url, jR)

    val sTime : String = (jV \\ "minTime").extract[String]
    val eTime : String = (jV \\ "maxTime").extract[String]

    val endDate = DateTime.parse(eTime).plusSeconds(1)

    new Interval(DateTime.parse(sTime), endDate)
  }

  @throws(classOf[DruidDataSourceException])
  def metadata(dataSource : String, fullIndex : Boolean) : DruidDataSource = {
    metadata(url, dataSource, null, fullIndex)
  }

  @throws(classOf[DruidDataSourceException])
  def segmentInfo(dataSource : String) : List[SegmentInfo] = {

    val in = timeBoundary(dataSource)

    val t = render(("type" -> "none"))

    val jR = render(
      ("queryType" -> "segmentMetadata") ~ ("dataSource" -> dataSource) ~
        ("intervals" -> List(in.toString)) ~
        ("merge" -> "false") ~
        ("toInclude" -> render(("type" -> "none")))
    )
    val jV = post(url, jR) transformField {
      case ("type", x) => ("typ", x)
    }

    val l = jV.extract[List[MetadataResponse]]
    l.map(new SegmentInfo(_))
  }

  @throws(classOf[DruidDataSourceException])
  def executeQuery(qry : QuerySpec) : List[ResultRow] = {
    executeQuery(url, qry)
  }

  @throws(classOf[DruidDataSourceException])
  def executeQueryAsStream(qry : QuerySpec) : CloseableIterator[ResultRow] = {
    executeQueryAsStream(url, qry)
  }
}

class DruidCoordinatorClient(host : String, port : Int, useSmile : Boolean = false)
  extends DruidClient(host, port, useSmile) with Logging {

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

  @throws(classOf[DruidDataSourceException])
  def metadataFromHistorical(histServer : HistoricalServerInfo,
                             dataSource : String,
                             fullIndex : Boolean) : DruidDataSource = {
    val (h,p) = DruidClient.hosPort(histServer.host)
    val url = s"http://$h:$p/druid/v2/?pretty"
    metadata(url, dataSource, histServer.segments.values.toList, fullIndex)
  }

  @throws(classOf[DruidDataSourceException])
  def serversInfo : List[HistoricalServerInfo] = {
    val url = s"$urlPrefix/servers?full=true"
    val jV = get(url)
    jV.extract[List[HistoricalServerInfo]]
  }

  @throws(classOf[DruidDataSourceException])
  def dataSourceInfo(datasource : String) : DataSourceSegmentInfo = {
    val url = s"$urlPrefix/datasources/$datasource?full=true"
    val jV = get(url)
    jV.extract[DataSourceSegmentInfo]
  }
}