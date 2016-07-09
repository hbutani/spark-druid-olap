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

package org.apache.spark.sql.sources.druid

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.joda.time.Interval
import org.sparklinedata.druid.{QuerySpec, TimeSeriesQuerySpec}
import org.sparklinedata.druid.metadata.{DruidMetadataCache, DruidRelationInfo}

trait DruidQueryCost extends Ordered[DruidQueryCost] {
  def queryCost : Double

  override def compare(that: DruidQueryCost): Int =
    if ( this.queryCost < that.queryCost) {
      Math.max(this.queryCost - that.queryCost, Int.MinValue.toDouble).toInt
    } else {
      Math.min(this.queryCost - that.queryCost, Int.MaxValue.toDouble).toInt
    }
}

case class BrokerQueryCost(
                            numWaves: Long,
                            processingCostPerHist: Double,
                            brokerMergeCost: Double,
                            segmentOutputTransportCost : Double,
                            queryCost: Double
                          ) extends DruidQueryCost {

  override def toString : String = {
    s"""
       |numWaves = $numWaves,
       |processingCostPerHist = $processingCostPerHist,
       |brokerMergeCost = $brokerMergeCost,
       |segmentOutputTransportCost = $segmentOutputTransportCost
       |queryCost = $queryCost
     """.stripMargin
  }
}

case class HistoricalQueryCost(
                         numWaves: Long,
                         estimateOutputSizePerHist: Long,
                         processingCostPerHist: Double,
                         histMergeCost: Double,
                         segmentOutputTransportCost: Double,
                         shuffleCost: Double,
                         sparkSchedulingCost: Double,
                         sparkAggCost: Double,
                         costPerHistoricalWave: Double,
                         druidStageCost: Double,
                         queryCost: Double
                       ) extends DruidQueryCost {

  def this() = this(
    -1, -1L, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, Double.MaxValue
  )

  override def toString : String = {
    s"""
       |numWaves = $numWaves,
       |estimateOutputSizePerHist = $estimateOutputSizePerHist,
       |processingCostPerHist = $processingCostPerHist,
       |histMergeCost = $histMergeCost,
       |segmentOutputTransportCost = $segmentOutputTransportCost,
       |shuffleCost = $shuffleCost,
       |sparkSchedulingCost = $sparkSchedulingCost,
       |sparkAggCost = $sparkAggCost,
       |costPerHistoricalWave = $costPerHistoricalWave,
       |druidStageCost = $druidStageCost,
       |queryCost = $queryCost
     """.stripMargin
  }
}

case class DruidQueryMethod(
                              queryHistorical : Boolean,
                              numSegmentsPerQuery : Int,
                              cost : DruidQueryCost
                              )

case class CostInput[T <: QuerySpec](
                      dimsNDVEstimate : Int,
                      shuffleCostPerByte : Double,
                      histMergeCostPerByteFactor : Double,
                      histSegsPerQueryLimit : Int,
                      brokerSizeThreshold : Int,
                      queryIntervalRatioScaleFactor : Double,
                      historicalTimeSeriesProcessingCostPerByteFactor : Double,
                      historicalGByProcessigCostPerByteFactor : Double,
                      sparkSchedulingCostPerTask : Double,
                      sparkAggregationCostPerByteFactor : Double,
                      druidOutputTransportCostPerByteFactor : Double,
                      indexIntervalMillis : Long,
                      queryIntervalMillis : Long,
                      segIntervalMillis : Long,
                      sparkCoresPerExecutor : Long,
                      numSparkExecutors : Int,
                      numProcessingThreadsPerHistorical : Long,
                      numHistoricals : Int,
                      querySpecClass : Class[T]
                    ) {

  // scalastyle:off line.size.limit
  override def toString : String = {
    s"""
       |dimsNDVEstimate = $dimsNDVEstimate
       |shuffleCostPerByte = $shuffleCostPerByte,
       |histMergeCostPerByteFactor = $histMergeCostPerByteFactor,
       |histSegsPerQueryLimit = $histSegsPerQueryLimit,
       |brokerSizeThreshold = $brokerSizeThreshold,
       |queryIntervalRatioScaleFactor = $queryIntervalRatioScaleFactor,
       |historicalTimeSeriesProcessingCostPerByteFactor = $historicalTimeSeriesProcessingCostPerByteFactor,
       |historicalGByProcessigCostPerByteFactor = $historicalGByProcessigCostPerByteFactor,
       |sparkSchedulingCostPerTask = $sparkSchedulingCostPerTask,
       |sparkAggregationCostPerByteFactor = $sparkAggregationCostPerByteFactor,
       |druidOutputTransportCostPerByteFactor = $druidOutputTransportCostPerByteFactor,
       |indexIntervalMillis = $indexIntervalMillis,
       |queryIntervalMillis = $queryIntervalMillis,
       |segIntervalMillis = $segIntervalMillis,
       |sparkCoresPerExecutor = $sparkCoresPerExecutor,
       |numSparkExecutors = $numSparkExecutors,
       |numProcessingThreadsPerHistorical = $numProcessingThreadsPerHistorical,
       |numHistoricals = $numHistoricals,
       |querySpecClass = $querySpecClass
     """.stripMargin
  }
  // scalastyle:on
}

object DruidQueryCostModel extends Logging {

  def intervalsMillis(intervals : List[Interval]) : Long = {
    intervals.foldLeft(0L) {
      case (t, i) => t + (i.getEndMillis - i.getStartMillis)
    }
  }

  def intervalNDVEstimate(
                         intervalMillis : Long,
                         totalIntervalMillis : Long,
                         ndvForIndexEstimate : Long,
                         queryIntervalRatioScaleFactor : Double
                         ) : Long = {
    val intervalRatio : Double = Math.min(intervalMillis/totalIntervalMillis, 1.0)
    val scaledRatio = Math.max(queryIntervalRatioScaleFactor * intervalRatio * 10.0, 10.0)
    Math.round(ndvForIndexEstimate * Math.log10(scaledRatio))
  }

  private[druid] def compute[T <: QuerySpec](cI : CostInput[T]) : DruidQueryMethod = {
    import cI._

    val histMergeCostPerByte = shuffleCostPerByte * histMergeCostPerByteFactor
    val brokerMergeCostPerByte = shuffleCostPerByte * histMergeCostPerByteFactor
    val histProcessingCostPerByte = {
      if (classOf[TimeSeriesQuerySpec].isAssignableFrom(querySpecClass)) {
        historicalTimeSeriesProcessingCostPerByteFactor
      } else {
        historicalGByProcessigCostPerByteFactor
      } * shuffleCostPerByte
    }

    val queryOutputSizeEstimate = intervalNDVEstimate(queryIntervalMillis,
      indexIntervalMillis,
      dimsNDVEstimate,
      queryIntervalRatioScaleFactor
    )

    val segmentOutputSizeEstimate = intervalNDVEstimate(segIntervalMillis,
      indexIntervalMillis,
      dimsNDVEstimate,
      queryIntervalRatioScaleFactor
    )

    val numSegmentsProcessed : Long =
      Math.max(Math.round(queryIntervalMillis/segIntervalMillis + 0.5), 1L)

    val numSparkCores : Long = {
      numSparkExecutors * sparkCoresPerExecutor
    }

    val numHistoricalThreads = numHistoricals * numProcessingThreadsPerHistorical

    val parallelismPerWave = Math.min(numHistoricalThreads, numSparkCores)

    def estimateNumWaves(numSegsPerQuery : Long) : Long =
      Math.round(
        (numSegmentsProcessed/numSegsPerQuery)/ parallelismPerWave + 0.5
      )

    def brokerQueryCost : DruidQueryCost = {
      val numWaves: Long = estimateNumWaves(1)
      val processingCostPerHist : Double =
        segmentOutputSizeEstimate * histProcessingCostPerByte
      var brokertMergeCost : Double =
        (numSegmentsProcessed - 1) * segmentOutputSizeEstimate * brokerMergeCostPerByte
      val segmentOutputTransportCost = queryOutputSizeEstimate *
        (druidOutputTransportCostPerByteFactor * shuffleCostPerByte)
      val queryCost: Double =
        numWaves * processingCostPerHist + segmentOutputTransportCost + brokertMergeCost

      BrokerQueryCost(
        numWaves,
        processingCostPerHist,
        brokertMergeCost,
        segmentOutputTransportCost,
        queryCost
      )
    }

    def histQueryCost(numSegsPerQuery : Long) : DruidQueryCost = {

      val numWaves = estimateNumWaves(numSegsPerQuery)
      val estimateOutputSizePerHist = intervalNDVEstimate(
        segIntervalMillis * numSegsPerQuery,
        indexIntervalMillis,
        dimsNDVEstimate,
        queryIntervalRatioScaleFactor
      )

      val processingCostPerHist : Double =
        numSegsPerQuery * segmentOutputSizeEstimate * histProcessingCostPerByte

      val histMergeCost : Double =
        (numSegsPerQuery - 1) * segmentOutputSizeEstimate * histMergeCostPerByte

      val segmentOutputTransportCost = estimateOutputSizePerHist *
        (druidOutputTransportCostPerByteFactor * shuffleCostPerByte)

      val shuffleCost = numWaves * segmentOutputSizeEstimate * shuffleCostPerByte

      val sparkSchedulingCost =
        numWaves * Math.min(parallelismPerWave, numSegmentsProcessed) * sparkSchedulingCostPerTask

      val sparkAggCost = numWaves * segmentOutputSizeEstimate *
        (sparkAggregationCostPerByteFactor * shuffleCostPerByte)

      val costPerHistoricalWave = processingCostPerHist + histMergeCost + segmentOutputTransportCost

      val druidStageCost = numWaves * costPerHistoricalWave

      val queryCost = druidStageCost + shuffleCost + sparkSchedulingCost + sparkAggCost

      HistoricalQueryCost(
        numWaves,
        estimateOutputSizePerHist,
        processingCostPerHist,
        histMergeCost,
        segmentOutputTransportCost,
        shuffleCost,
        sparkSchedulingCost,
        sparkAggCost,
        costPerHistoricalWave,
        druidStageCost,
        queryCost
      )
    }

    log.info(
      s"""Druid Query Cost Model Input:
         |$cI
         |histProcessingCost = $histProcessingCostPerByte
         |queryOutputSizeEstimate = $queryOutputSizeEstimate
         |segmentOutputSizeEstimate = $segmentOutputSizeEstimate
         |numSegmentsProcessed = $numSegmentsProcessed
         |numSparkCores = $numSparkCores
         |numHistoricalThreads = $numHistoricalThreads
         |parallelismPerWave = $parallelismPerWave
         |
       """.stripMargin)

    var minCost : DruidQueryCost = brokerQueryCost
    var minNumSegmentsPerQuery = -1

    log.info(
      s"""Druid Query Cost Model Cost for broker query =
          |$minCost""".stripMargin)

    (1 to histSegsPerQueryLimit).foreach { (numSegsPerQuery : Int) =>
      val c = histQueryCost(numSegsPerQuery)
      log.info(
        s"""Druid Query Cost Model Cost for numSegsPerQuery = $numSegsPerQuery:
           |$c""".stripMargin)
      if ( c < minCost ) {
        minCost = c
        minNumSegmentsPerQuery = numSegsPerQuery
      }
    }
    log.info(
      s"""Druid Query Cost Model Output:
         |numSegsPerQuery= $minNumSegmentsPerQuery
         | cost = (
         | $minCost
         | )""".stripMargin)
    DruidQueryMethod(minCost.isInstanceOf[HistoricalQueryCost], minNumSegmentsPerQuery, minCost)

  }

  def computeMethod[T <: QuerySpec](
                   sqlContext : SQLContext,
                   drInfo : DruidRelationInfo,
                   dimsNDVEstimate : Int,
                   queryIntervalMillis : Long,
                   querySpecClass : T
                   ) : DruidQueryMethod = {

    val shuffleCostPerByte: Double = 1.0

    val histMergeFactor = sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_HIST_MERGE_COST)
    val histMergeCostPerByte = shuffleCostPerByte * histMergeFactor

    val histSegsPerQueryLimit =
      sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_HIST_SEGMENTS_PERQUERY_LIMIT)

    val brokerSizeThreshold =
      sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_BROKER_SIZE_THRESHOLD)

    val queryIntervalRatioScaleFactor =
      sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_INTERVAL_SCALING_NDV)

    val historicalTimeSeriesProcessingCostPerByte = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_HISTORICAL_TIMESERIES_PROCESSING_COST)

    val historicalGByProcessigCostPerByte = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_HISTORICAL_PROCESSING_COST)

    val sparkScheulingCostPerTask = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_SPARK_SCHEDULING_COST)

    val sparkAggregationCostPerByte = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_SPARK_AGGREGATING_COST)

    val druidOutputTransportCostPerByte = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_OUTPUT_TRANSPORT_COST)

    val indexIntervalMillis = intervalsMillis(drInfo.druidDS.intervals)
    val segIntervalMillis = {
      val segIn = DruidMetadataCache.getDataSourceInfo(
        drInfo.fullName, drInfo.options)._1.segments.head._interval
      intervalsMillis(List(segIn))
    }

    val queryOutputSizeEstimate = intervalNDVEstimate(queryIntervalMillis,
      indexIntervalMillis,
      dimsNDVEstimate,
      queryIntervalRatioScaleFactor
    )

    val segmentOutputSizeEstimate = intervalNDVEstimate(segIntervalMillis,
      indexIntervalMillis,
      dimsNDVEstimate,
      queryIntervalRatioScaleFactor
    )

    val numSegmentsProcessed: Long =
      Math.max(Math.round(queryIntervalMillis / segIntervalMillis), 1L)

    val sparkCoresPerExecutor =
      sqlContext.sparkContext.getConf.get("spark.executor.cores").toLong

    val numSparkExecutors = sqlContext.sparkContext.getExecutorMemoryStatus.size

    val numProcessingThreadsPerHistorical: Long =
      drInfo.options.
        numProcessingThreadsPerHistorical.map(_.toLong).getOrElse(sparkCoresPerExecutor)

    val numHistoricals = {
      DruidMetadataCache.getDruidClusterInfo(
        drInfo.fullName, drInfo.options).histServers.size
    }

    compute(
      CostInput(
        dimsNDVEstimate,
        shuffleCostPerByte,
        histMergeFactor,
        histSegsPerQueryLimit,
        brokerSizeThreshold,
        queryIntervalRatioScaleFactor,
        historicalTimeSeriesProcessingCostPerByte,
        historicalGByProcessigCostPerByte,
        sparkScheulingCostPerTask,
        sparkAggregationCostPerByte,
        druidOutputTransportCostPerByte,
        indexIntervalMillis,
        queryIntervalMillis,
        segIntervalMillis,
        sparkCoresPerExecutor,
        numSparkExecutors,
        numProcessingThreadsPerHistorical,
        numHistoricals,
        querySpecClass.getClass
      )
    )
  }
}
