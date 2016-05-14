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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRowWithSchema
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.joda.time.Interval
import org.sparklinedata.druid.client.{DruidQueryServerClient, QueryResultRow}
import org.sparklinedata.druid.metadata.DruidMetadataCache
import org.sparklinedata.druid.metadata.DruidRelationInfo
import org.sparklinedata.druid.metadata.DruidSegmentInfo
import org.sparklinedata.druid.metadata.HistoricalServerAssignment

import scala.util.Random

abstract class DruidPartition extends Partition {
  def queryClient : DruidQueryServerClient
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

class HistoricalPartition(idx: Int, val hs : HistoricalServerAssignment) extends DruidPartition {
  override def index: Int = idx
  def queryClient : DruidQueryServerClient = new DruidQueryServerClient(hs.server.host)

  def intervals : List[Interval] = hs.segmentIntervals.map(_._2)

  def segIntervals : List[(DruidSegmentInfo, Interval)] = hs.segmentIntervals
}

class BrokerPartition(idx: Int,
                      val broker : String,
                      val i : Interval) extends DruidPartition {
  override def index: Int = idx
  def queryClient : DruidQueryServerClient = new DruidQueryServerClient(broker)
  def intervals : List[Interval] = List(i)

  def segIntervals : List[(DruidSegmentInfo, Interval)] = null
}


class DruidRDD(sqlContext: SQLContext,
              val drInfo : DruidRelationInfo,
                val dQuery : DruidQuery)  extends  RDD[InternalRow](sqlContext.sparkContext, Nil) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {


    val p = split.asInstanceOf[DruidPartition]
    val mQry = p.setIntervalsOnQuerySpec(dQuery.q)
    Utils.logQuery(mQry)
    val client = p.queryClient
    val dr = client.executeQueryAsStream(mQry)
    context.addTaskCompletionListener{ context => dr.closeIfNeeded() }
    val r = new InterruptibleIterator[QueryResultRow](context, dr)
    val schema = dQuery.schema(drInfo)
    val nameToTF = dQuery.getValTFMap
    r.map { r =>
      new GenericInternalRowWithSchema(schema.fields.map
      (f => DruidValTransform.sparkValue(
        r.event(f.name), nameToTF.get(f.name).getOrElse(""))), schema)
    }
  }

  override protected def getPartitions: Array[Partition] = {
    if (dQuery.queryHistoricalServer) {
    val hAssigns = DruidMetadataCache.assignHistoricalServers(
      drInfo.fullName,
      drInfo.options,
      dQuery.intervalSplits
    )
      var idx = -1
      val numSegmentsPerQuery = drInfo.options.numSegmentsPerHistoricalQuery

      val l  = (for(
        hA <- hAssigns;
           segIns <- hA.segmentIntervals.sliding(numSegmentsPerQuery,numSegmentsPerQuery)
      ) yield {
        idx = idx + 1
        new HistoricalPartition(idx, new HistoricalServerAssignment(hA.server, segIns))
      }
        )

      val l1 : Array[Partition] = Random.shuffle(l).toArray
      l1
  } else {
      val broker = DruidMetadataCache.getDruidClusterInfo(drInfo.fullName,
        drInfo.options).curatorConnection.getBroker
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

  // TODO: create an enum of TFs
  private[this] val tfMap: Map[String, Any => Any] = Map[String, Any => Any](
    "toTSWithTZAdj" -> toTSWithTZAdj,
    "toTS" -> toTS,
    "toString" -> toString,
    "toInt" -> toInt,
    "toLong" -> toLong,
    "toFloat" -> toFloat
  )

  def sparkValue(druidVal: Any, tfName: String): Any = {
    var tDVal = druidVal
    for (tf <- tfMap.get(tfName))
      tDVal = tf(druidVal)
    tDVal
  }

  def getTFName(sparkDT: DataType, adjForTZ: Boolean = false): String = sparkDT match {
    case TimestampType if adjForTZ => "toTSWithTZ"
    case TimestampType if !adjForTZ => "toTS"
    case StringType if !adjForTZ => "toString"
    case ShortType | IntegerType => "toInt"
    case LongType => "toLong"
    case FloatType => "toFloat"
    case _ => ""
  }
}
