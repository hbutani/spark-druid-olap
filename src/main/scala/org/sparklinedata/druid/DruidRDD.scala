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

package org.sparklinedata.druid

import org.apache.http.client.methods.{HttpExecutionAware, HttpRequestBase}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRowWithSchema
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.sql.SPLLogging

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.druid.{DruidPlanner, DruidQueryCostModel, DummyResultIterator}
import org.apache.spark.sql.sparklinedata.execution.metrics.DruidQueryExecutionMetric
import org.joda.time.Interval
import org.sparklinedata.druid.client.{CancellableHolder, ConnectionManager, DruidQueryServerClient, ResultRow}
import org.sparklinedata.druid.metadata._

import scala.collection._
import scala.collection.convert.decorateAsScala._
import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.core.Base64Variants
import org.apache.http.concurrent.Cancellable

import scala.util.Random

abstract class DruidPartition extends Partition {
  def queryClient(useSmile : Boolean,
                  httpMaxPerRoute : Int, httpMaxTotal : Int) : DruidQueryServerClient
  def intervals : List[Interval]
  def segIntervals : List[(DruidSegmentInfo, Interval)]

  def setIntervalsOnQuerySpec(q : QuerySpec) : QuerySpec = {
    if ( segIntervals == null) {
      q.setIntervals(intervals)
    } else {
      q.setSegIntervals(segIntervals)
    }
  }
}

class HistoricalPartition(idx: Int, hs : HistoricalServerAssignment) extends DruidPartition {
  private[druid] var _index: Int = idx
  def index = _index
  val hsName = hs.server.host

  def queryClient(useSmile : Boolean,
                  httpMaxPerRoute : Int, httpMaxTotal : Int) : DruidQueryServerClient = {
    ConnectionManager.init(httpMaxPerRoute, httpMaxTotal)
    new DruidQueryServerClient(hsName, useSmile)
  }

  val intervals : List[Interval] = hs.segmentIntervals.map(_._2)

  val segIntervals : List[(DruidSegmentInfo, Interval)] = hs.segmentIntervals

  override def setIntervalsOnQuerySpec(q : QuerySpec) : QuerySpec = {
    val r = super.setIntervalsOnQuerySpec(q)
    if (r.context.isDefined) {
      r.context.get.queryId = s"${r.context.get.queryId}-${index}"
    }
    r
  }
}

class BrokerPartition(idx: Int,
                      val broker : String,
                      val i : Interval) extends DruidPartition {
  override def index: Int = idx
  def queryClient(useSmile : Boolean,
                  httpMaxPerRoute : Int, httpMaxTotal : Int)  : DruidQueryServerClient = {
    ConnectionManager.init(httpMaxPerRoute, httpMaxTotal)
    new DruidQueryServerClient(broker, useSmile)
  }

  def intervals : List[Interval] = List(i)

  def segIntervals : List[(DruidSegmentInfo, Interval)] = null
}

class DruidRDD(sqlContext: SQLContext,
               drInfo : DruidRelationInfo,
                val dQuery : DruidQuery)  extends  RDD[InternalRow](sqlContext.sparkContext, Nil) {

  val recordDruidQuery = DruidPlanner.getConfValue(sqlContext,
    DruidPlanner.DRUID_RECORD_QUERY_EXECUTION
  )
  val druidQueryAcc : DruidQueryExecutionMetric = if (recordDruidQuery) {
    new DruidQueryExecutionMetric()
  } else {
    null
  }
  val numSegmentsPerQuery = dQuery.numSegmentsPerQuery
  val schema = dQuery.schema(drInfo)
  val useSmile = dQuery.useSmile && smileCompatible(schema)
  val drOptions = drInfo.options
  val drFullName = drInfo.fullName
  val drDSIntervals = drInfo.druidDS.intervals
  val inputEstimate = DruidQueryCostModel.estimateInput(dQuery.q, drInfo)
  val outputEstimate = DruidQueryCostModel.estimateOutput(dQuery.q, drInfo)
  val (httpMaxPerRoute, httpMaxTotal) = (
    DruidPlanner.getConfValue(sqlContext,
      DruidPlanner.DRUID_CONN_POOL_MAX_CONNECTIONS_PER_ROUTE),
    DruidPlanner.getConfValue(sqlContext,
      DruidPlanner.DRUID_CONN_POOL_MAX_CONNECTIONS)
    )

  val sparkToDruidColName: Map[String, String] =
    dQuery.q.mapSparkColNameToDruidColName(drInfo).map(identity)
  // scalastyle:off line.size.limit
  // why map identity?
  // see http://stackoverflow.com/questions/17709995/notserializableexception-for-mapstring-string-alias
  // scalastyle:on line.size.limit

  def druidColName(n : String) = sparkToDruidColName.getOrElse(n, n)

  /*
   * for now if there is a binary field don't use Smile.
   */
  def smileCompatible(typ : StructType) : Boolean = {
    typ.fields.foldLeft(true) {
      case (false, _) => false
      case (_, StructField(_, BinaryType, _, _)) => false
      case (_, StructField(_, st:StructType, _,_)) => smileCompatible(st)
      case _  => true
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val p = split.asInstanceOf[DruidPartition]
    val mQry = p.setIntervalsOnQuerySpec(dQuery.q)
    Utils.logQuery(mQry)
    /*
     * Druid Query execution steps:
     * 1. Register queryId with TaskCancelHandler
     * 2. Provide the [[org.sparklinedata.druid.client.CancellableHolder]] to the DruidClient,
     *    so it can relay any [[org.apache.http.concurrent.Cancellable]] resources to
     *    the holder.
     * 3. If an error occurs and the holder's ''wasCancelTriggered'' flag is set, a
     *    DummyResultIterator is returned. By this time the Task has been cancelled by
     *    the ''Spark Executor'', so it is safe to return an empty iterator.
     * 4. Always clear this Druid Query from the TaskCancelHandler
     */
    var cancelCallback : TaskCancelHandler.TaskCancelHolder = null
    var dr: CloseableIterator[ResultRow] = null
    var client : DruidQueryServerClient = null
    val qryId = mQry.context.map(_.queryId).getOrElse(s"q-${System.nanoTime()}")
    var qrySTime = System.currentTimeMillis()
    var qrySTimeStr = s"${new java.util.Date()}"
    try {
      cancelCallback = TaskCancelHandler.registerQueryId(qryId, context)
      client = p.queryClient(useSmile, httpMaxPerRoute, httpMaxTotal)
      client.setCancellableHolder(cancelCallback)
      qrySTime = System.currentTimeMillis()
      qrySTimeStr = s"${new java.util.Date()}"
      dr = mQry.executeQuery(client)
    } catch {
      case _ if cancelCallback.wasCancelTriggered && client != null => {
        dr = new DummyResultIterator()
      }
      case e: Throwable => throw e
    }
    finally {
      TaskCancelHandler.clearQueryId(qryId)
    }

    val druidExecTime = (System.currentTimeMillis() - qrySTime)
    var numRows : Int = 0

    context.addTaskCompletionListener{ context =>
      val queryExecTime = (System.currentTimeMillis() - qrySTime)
      if (recordDruidQuery) {
        druidQueryAcc.add(
          DruidQueryExecutionView(
            context.stageId,
            context.partitionId(),
            context.taskAttemptId(),
            s"${client.host}:${client.port}",
            if (p.segIntervals == null) None
            else {
              Some(
                p.segIntervals.map(t => (t._1.identifier, t._2.toString))
              )
            },
            qrySTimeStr,
            druidExecTime,
            queryExecTime,
            numRows,
            Utils.queryToString(mQry)
          )
        )
      }
      dr.closeIfNeeded()
    }

    val r = new InterruptibleIterator[ResultRow](context, dr)
    val nameToTF = dQuery.getValTFMap

    /*
     * multiple output fields may project on the same druid column with different
     * transformations. True for the dimensions of a spatialIndex point.
     */
    def tfName(f : StructField) : Option[String] = {
      if (nameToTF.contains(f.name)) {
        nameToTF.get(f.name)
      } else {
        nameToTF.get(druidColName(f.name))
      }
    }

    r.map { r =>
      numRows += 1
      val row = new GenericInternalRowWithSchema(schema.fields.map
      (f => DruidValTransform.sparkValue(
        f, r.event(druidColName(f.name)), tfName(f))), schema)
      row
    }
  }

  override protected def getPartitions: Array[Partition] = {
    if (dQuery.queryHistoricalServer) {
    val hAssigns = DruidMetadataCache.assignHistoricalServers(
      drFullName,
      drOptions,
      dQuery.intervalSplits
    )
      var idx = -1

      val l  = (for(
        hA <- hAssigns;
           segIns <- hA.segmentIntervals.sliding(numSegmentsPerQuery,numSegmentsPerQuery)
      ) yield {
        idx = idx + 1
        new HistoricalPartition(idx, new HistoricalServerAssignment(hA.server, segIns))
      }
        )

      val l1 : Array[Partition] = Random.shuffle(l).zipWithIndex.map{t =>
        val p = t._1
        val i = t._2
        p._index = i
        p
      }.toArray

      l1
  } else {
      // ensure DataSource is in the Metadata Cache.
      DruidMetadataCache.getDataSourceInfo(drFullName, drOptions)
      val broker = DruidMetadataCache.getDruidClusterInfo(drFullName,
        drOptions).curatorConnection.getBroker
      dQuery.intervalSplits.zipWithIndex.map(t => new BrokerPartition(t._2, broker, t._1)).toArray
    }
  }
}

/**
  * conversion from Druid values to Spark values. Most of the conversion cases are handled by
  * cast expressions in the [[org.apache.spark.sql.execution.Project]] operator above the
  * DruidRelation Operator; but some values needs massaging like TimeStamps, Strings...
  */
object DruidValTransform {

  private[this] val dTZ = org.joda.time.DateTimeZone.getDefault

  private[this] val toTSWithTZAdj = (druidVal: Any) => {
    val dvLong = if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toLong
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toLong
    } else if (druidVal.isInstanceOf[String]){
      druidVal.asInstanceOf[String].toLong
    }else {
      druidVal
    }

    new org.joda.time.DateTime(dvLong, dTZ).getMillis() * 1000.asInstanceOf[SQLTimestamp]
  }

  private[this] val toTS = (druidVal: Any) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].longValue().asInstanceOf[SQLTimestamp]
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toLong.asInstanceOf[SQLTimestamp]
    } else {
      druidVal
    }
  }

  private[this] val toString = (druidVal: Any) => {
    UTF8String.fromString(druidVal.toString)
  }

  private[this] val toInt = (druidVal: Any) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toInt
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toInt
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toInt
    }else {
      druidVal
    }
  }

  private[this] val toLong = (druidVal: Any) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toLong
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toLong
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toLong
    }else {
      druidVal
    }
  }

  private[this] val toFloat = (druidVal: Any) => {
    if (druidVal.isInstanceOf[Double]) {
      druidVal.asInstanceOf[Double].toFloat
    } else if (druidVal.isInstanceOf[BigInt]) {
      druidVal.asInstanceOf[BigInt].toFloat
    } else if (druidVal.isInstanceOf[String]) {
      druidVal.asInstanceOf[String].toFloat
    }else {
      druidVal
    }
  }

  private[this] def pointToDouble(dim : Int)(druidVal : Any) : Any = {
    if (druidVal == null) return null
    val point : Array[Double] = druidVal.toString.split(",").map(_.toDouble)
    if ( dim >= 0 && dim < point.length ) {
      point(dim)
    } else null
  }

  def dimConversion(i : Int) : String = {
    s"dim$i"
  }

  /**
    * conversion from Druid values to Spark values. Most of the conversion cases are handled by
    * cast expressions in the [[org.apache.spark.sql.execution.Project]] operator above the
    * DruidRelation Operator; but Strings need to be converted to [[UTF8String]] strings.
    *
    * @param f
    * @param druidVal
    * @return
    */
  def defaultValueConversion(f : StructField, druidVal : Any) : Any = f.dataType match {
    case TimestampType if druidVal.isInstanceOf[Double] =>
      druidVal.asInstanceOf[Double].longValue().asInstanceOf[SQLTimestamp]
    case StringType if druidVal != null => UTF8String.fromString(druidVal.toString)
    case LongType if druidVal.isInstanceOf[BigInt] =>
      druidVal.asInstanceOf[BigInt].longValue()
    case LongType if druidVal.isInstanceOf[Double] =>
      druidVal.asInstanceOf[Double].longValue()
    case BinaryType if druidVal.isInstanceOf[String] => {
      Base64Variants.getDefaultVariant.decode(druidVal.asInstanceOf[String])
    }
    case _ => druidVal
  }

  // TODO: create an enum of TFs
  private[this] val tfMap: Map[String, Any => Any] = {
    Map[String, Any => Any](
      "toTSWithTZAdj" -> toTSWithTZAdj,
      "toTS" -> toTS,
      "toString" -> toString,
      "toInt" -> toInt,
      "toLong" -> toLong,
      "toFloat" -> toFloat
    ) ++ (0 to 20).map { i =>
      s"dim$i" -> pointToDouble(i) _
    }
  }

  def sparkValue(f : StructField, druidVal: Any, tfName: Option[String]): Any = {
    tfName match {
      case Some(tf) if (tfMap.contains(tf) && druidVal != null) => tfMap(tf)(druidVal)
      case _ => defaultValueConversion(f, druidVal)
    }
  }

  def getTFName(sparkDT: DataType, adjForTZ: Boolean = false): String = sparkDT match {
    case TimestampType if adjForTZ => "toTSWithTZAdj"
    case TimestampType if !adjForTZ => "toTS"
    case StringType if !adjForTZ => "toString"
    case ShortType | IntegerType => "toInt"
    case LongType => "toLong"
    case FloatType => "toFloat"
    case _ => ""
  }
}

/**
  * The '''TaskCancel Thread''' tracks the Spark tasks that are executing Druid Queries.
  * Periodically(currently every 5 secs) it checks if any of the Spark Tasks have been
  * cancelled and relays this to the current [[org.apache.http.concurrent.Cancellable]] associated
  * with the [[org.apache.http.client.methods.HttpExecutionAware]] connectio handling the
  * ''Druid Query''
  *
  */
object TaskCancelHandler extends SPLLogging {

  private val taskMap : concurrent.Map[String, (Cancellable, TaskCancelHolder, TaskContext)] =
    new ConcurrentHashMap[String, (Cancellable, TaskCancelHolder, TaskContext)]().asScala

  class TaskCancelHolder(val queryId : String,
                         val taskContext : TaskContext) extends CancellableHolder {
    def setCancellable(c : Cancellable) : Unit = {
      log.debug("set cancellable for query {}", queryId)
      taskMap(queryId) = (c, this, taskContext)
    }
    @volatile
    var wasCancelTriggered : Boolean = false
  }


  def registerQueryId(queryId : String, taskContext : TaskContext) : TaskCancelHolder = {
    log.debug("register query {}", queryId)
    new TaskCancelHolder(queryId, taskContext)
  }

  def clearQueryId(queryId : String) : Unit = taskMap.remove(queryId)

  val secs5 : Long = 5 * 1000

  object cancelCheckThread extends Runnable with SPLLogging {

    def run() : Unit = {

      while(true) {
        Thread.sleep(secs5)
        log.debug(s"cancelThread woke up")
        taskMap.foreach{t =>
          val (queryId, (req, cancellableHolder, taskContext)) = t
          log.debug(s"checking task stageid=${taskContext.stageId()}, " +
            s"partitionId=${taskContext.partitionId()}, " +
            s"isInterrupted=${taskContext.isInterrupted()}")
          if (taskContext.isInterrupted()) {
            try {
              cancellableHolder.wasCancelTriggered = true
              req.cancel()
              log.info("aborted http request for query {}: {}", Array[Any](queryId, req))
            } catch {
              case e : Throwable => log.warn("failed to abort http request: {}", req)
            }
          }

        }
      }

    }

  }

  {
    val t = new Thread(cancelCheckThread)
    t.setName("DruidRDD-TaskCancelCheckThread")
    t.setDaemon(true)
    t.start()
  }


}