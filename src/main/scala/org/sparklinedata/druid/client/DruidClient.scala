package org.sparklinedata.druid.client

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.http.client.methods._
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.Logging
import org.joda.time.{DateTime, Interval}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.DruidMetadata

import scala.util.Try

class DruidException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(message : String) = this(message, null)
}


object ConnectionManager {
  val pool = new PoolingHttpClientConnectionManager
}

class DruidClient(val host : String, val port : Int) extends Logging {

  @transient val url = s"http://$host:$port/druid/v2/?pretty"

  private def httpClient: CloseableHttpClient = {
    HttpClients.custom.setConnectionManager(ConnectionManager.pool).build
  }

  private def release(resp: CloseableHttpResponse) : Unit = {
    Try {
      if (resp != null) EntityUtils.consume(resp.getEntity)
    } recover {
      case ex => log.error("Error returning client to pool", ExceptionUtils.getStackTrace(ex))
    }
  }

  private def getRequest(url : String) = new HttpGet(url)
  private def postRequest(url : String) = new HttpPost(url)

  @throws(classOf[DruidException])
  private def perform(reqType : String => HttpRequestBase,
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
          throw new DruidException(s"Unexpected response status: ${r.getStatusLine}")
        }
      }
      j <- Try {parse(respStr)}
    } yield j

    release(resp)
    js getOrElse( js.failed.get match {
      case dE : DruidException => throw dE
      case x => throw new DruidException("Failed in communication with Druid", x)
    })
  }

  private def post(payload : String,
                   reqHeaders: Map[String, String] = null ) : JValue =
    perform(postRequest _, payload, reqHeaders)

  private def get(payload : String,
                   reqHeaders: Map[String, String] = null) : JValue =
    perform(getRequest _, payload, reqHeaders)

  private def addHeaders(req: HttpRequestBase, reqHeaders: Map[String, String]) {
    if (reqHeaders == null) return
    for (key <- reqHeaders.keySet) {
      req.setHeader(key, reqHeaders(key))
    }
  }

  @throws(classOf[DruidException])
  def timeBoundary(dataSource : String) : Interval = {
    import org.json4s.JsonDSL._
    implicit val formats = DefaultFormats

    val jR = compact(render(
      ( "queryType" -> "timeBoundary") ~ ("dataSource" -> dataSource)
    ))
    val jV = post(jR)

    val sTime : String = (jV \\ "minTime").extract[String]
    val eTime : String = (jV \\ "maxTime").extract[String]
    new Interval(DateTime.parse(sTime), DateTime.parse(eTime))
  }

  @throws(classOf[DruidException])
  def metadata(dataSource : String) : DruidMetadata = {
    import org.json4s.JsonDSL._
    implicit val formats = org.json4s.DefaultFormats ++ org.json4s.ext.JodaTimeSerializers.all

    val i = timeBoundary(dataSource).toString

    val jR = compact(render(
      ("queryType" -> "segmentMetadata") ~ ("dataSource" -> dataSource) ~
        ("intervals" -> List(i)) ~ ("merge" -> "true")
    ))
    val jV = post(jR) transformField {
      case ("type", x) => ("typ", x)
    }

    val l = jV.extract[List[MetadataResponse]]
    DruidMetadata(dataSource, l.head)
  }
}
