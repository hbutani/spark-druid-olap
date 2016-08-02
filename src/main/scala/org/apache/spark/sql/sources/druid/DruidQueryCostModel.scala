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

import java.text.DecimalFormat

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.joda.time.Interval
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata._
import org.apache.commons.lang3.StringUtils

import scala.language.existentials
import scala.collection.mutable.{ArrayBuffer, HashMap => MHashMap, Map => MMap}

sealed trait DruidQueryCost extends Ordered[DruidQueryCost] {
  def queryCost : Double

  override def compare(that: DruidQueryCost): Int =
    if ( this.queryCost < that.queryCost) {
      Math.max(this.queryCost - that.queryCost, Int.MinValue.toDouble).toInt
    } else {
      Math.min(this.queryCost - that.queryCost, Int.MaxValue.toDouble).toInt
    }
}

case class CostDetails(
                                        costInput : CostInput[_ <: QuerySpec],
                                        histProcessingCostPerRow : Double,
                                        queryOutputSizeEstimate : Long,
                                        segmentOutputSizeEstimate : Long,
                                        numSegmentsProcessed : Long,
                                        numSparkCores : Long,
                                        numHistoricalThreads : Long,
                                        parallelismPerWave : Long,
                                        minCost : DruidQueryCost,
                                        allCosts : List[DruidQueryCost]
                                      ) {


  override def toString : String = {
    val header = s"""Druid Query Cost Model::
                     |         |${costInput}histProcessingCost = $histProcessingCostPerRow
                     |         |queryOutputSizeEstimate = $queryOutputSizeEstimate
                     |         |segmentOutputSizeEstimate = $segmentOutputSizeEstimate
                     |         |numSegmentsProcessed = $numSegmentsProcessed
                     |         |numSparkCores = $numSparkCores
                     |         |numHistoricalThreads = $numHistoricalThreads
                     |         |parallelismPerWave = $parallelismPerWave
                     |
                     |         |minCost : $minCost
                     |         |
                     |       """.stripMargin



    val rows = allCosts.map(CostDetails.costRow(_)).mkString("", "\n", "")

    s"""${header}Cost Details:
       |${CostDetails.tableHeader}
       |$rows
     """.stripMargin
  }

}

object CostDetails {

  val formatter = new DecimalFormat("#######0.##E0")
  def roundValue(v : Double) : String = formatter.format(v)

  val tableHeaders = List(
    "broker/historical",
    "   queryCost   ",
    "numWaves",
    "numSegmentsPerQuery",
    "druidMergeCostPerWave",
    "transportCostPerWave",
    "druidCostPerWave",
    "   totalDruidCost    ",
    "   sparkShuffleCost   ",
    "   sparkAggCost   ",
    "   sparkSchedulingCost   "
  )

  val colWidths = tableHeaders.map(_.size)

  def tableHeader : String = {
    tableHeaders.mkString("", " | ", "")
  }

  def brokerCost(c : BrokerQueryCost) : String = {
    import c._
    val row = List(
      StringUtils.leftPad("broker", colWidths(0)),
      StringUtils.leftPad(roundValue(queryCost), colWidths(1)),
      StringUtils.leftPad("1", colWidths(2)),
      StringUtils.leftPad("all", colWidths(3)),
      StringUtils.leftPad(roundValue(brokerMergeCost), colWidths(4)),
      StringUtils.leftPad(roundValue(segmentOutputTransportCost), colWidths(5)),
      StringUtils.leftPad(roundValue(queryCost), colWidths(6)),
      StringUtils.leftPad(roundValue(queryCost), colWidths(7)),
      StringUtils.leftPad("0.0", colWidths(8)),
      StringUtils.leftPad("0.0", colWidths(9)),
      StringUtils.leftPad("0.0", colWidths(10))
    )
    row.mkString("", " | ", "")
  }

  def historicalCost(c : HistoricalQueryCost) : String = {
    import c._
    val row = List(
      StringUtils.leftPad("historical", colWidths(0)),
      StringUtils.leftPad(roundValue(queryCost), colWidths(1)),
      StringUtils.leftPad(numWaves.toString, colWidths(2)),
      StringUtils.leftPad(numSegmentsPerQuery.toString, colWidths(3)),
      StringUtils.leftPad(roundValue(histMergeCost), colWidths(4)),
      StringUtils.leftPad(roundValue(segmentOutputTransportCost), colWidths(5)),
      StringUtils.leftPad(roundValue(costPerHistoricalWave), colWidths(6)),
      StringUtils.leftPad(roundValue(druidStageCost), colWidths(7)),
      StringUtils.leftPad(roundValue(shuffleCost), colWidths(8)),
      StringUtils.leftPad(roundValue(sparkAggCost), colWidths(9)),
      StringUtils.leftPad(roundValue(sparkSchedulingCost), colWidths(10))
    )
    row.mkString("", " | ", "")
  }

  def costRow(c : DruidQueryCost) : String = c match {
    case br : BrokerQueryCost => brokerCost(br)
    case hr : HistoricalQueryCost => historicalCost(hr)
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
                         numSegmentsPerQuery : Long,
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

  override def toString : String = {
    s"""
       |numWaves = $numWaves,
       |numSegmentsPerQuery = $numSegmentsPerQuery,
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
                             bestCost : DruidQueryCost,
                             costDetails : CostDetails
                              ) {
  override def toString : String = {
    s"""
       |queryHistorical = $queryHistorical,
       |numSegmentsPerQuery = $numSegmentsPerQuery,
       |bestCost = ${bestCost.queryCost},
       |$costDetails
     """.stripMargin
  }
}

case class CostInput[T <: QuerySpec](
                      dimsNDVEstimate : Long,
                      shuffleCostPerRow : Double,
                      histMergeCostPerRowFactor : Double,
                      histSegsPerQueryLimit : Int,
                      queryIntervalRatioScaleFactor : Double,
                      historicalTimeSeriesProcessingCostPerRowFactor : Double,
                      historicalGByProcessigCostPerRowFactor : Double,
                      sparkSchedulingCostPerTask : Double,
                      sparkAggregationCostPerRowFactor : Double,
                      druidOutputTransportCostPerRowFactor : Double,
                      indexIntervalMillis : Long,
                      queryIntervalMillis : Long,
                      segIntervalMillis : Long,
                      numSegmentsProcessed: Long,
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
       |shuffleCostPerRow = $shuffleCostPerRow,
       |histMergeCostPerRowFactor = $histMergeCostPerRowFactor,
       |histSegsPerQueryLimit = $histSegsPerQueryLimit,
       |queryIntervalRatioScaleFactor = $queryIntervalRatioScaleFactor,
       |historicalTimeSeriesProcessingCostPerRowFactor = $historicalTimeSeriesProcessingCostPerRowFactor,
       |historicalGByProcessigCostPerRowFactor = $historicalGByProcessigCostPerRowFactor,
       |sparkSchedulingCostPerTask = $sparkSchedulingCostPerTask,
       |sparkAggregationCostPerRowFactor = $sparkAggregationCostPerRowFactor,
       |druidOutputTransportCostPerRowFactor = $druidOutputTransportCostPerRowFactor,
       |indexIntervalMillis = $indexIntervalMillis,
       |queryIntervalMillis = $queryIntervalMillis,
       |segIntervalMillis = $segIntervalMillis,
       |numSegmentsProcessed = $numSegmentsProcessed,
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

    val histMergeCostPerRow = shuffleCostPerRow * histMergeCostPerRowFactor
    val brokerMergeCostPerRow = shuffleCostPerRow * histMergeCostPerRowFactor
    val histProcessingCostPerRow = {
      if (classOf[TimeSeriesQuerySpec].isAssignableFrom(querySpecClass)) {
        historicalTimeSeriesProcessingCostPerRowFactor
      } else {
        historicalGByProcessigCostPerRowFactor
      } * shuffleCostPerRow
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

    val numSparkCores : Long = {
      numSparkExecutors * sparkCoresPerExecutor
    }

    val numHistoricalThreads = numHistoricals * numProcessingThreadsPerHistorical

    val parallelismPerWave = Math.min(numHistoricalThreads, numSparkCores)

    def estimateNumWaves(numSegsPerQuery : Long,
                         parallelism : Long = parallelismPerWave) : Long =
      Math.round(
        (numSegmentsProcessed/numSegsPerQuery)/ parallelism + 0.5
      )

    def brokerQueryCost : DruidQueryCost = {
      val numWaves: Long = estimateNumWaves(1, numHistoricalThreads)
      val processingCostPerHist : Double =
        segmentOutputSizeEstimate * histProcessingCostPerRow

      val numMerges = 2 * (numSegmentsProcessed - 1)

      val brokertMergeCost : Double =
        (Math.max(numMerges / numProcessingThreadsPerHistorical,1.0))  *
          segmentOutputSizeEstimate * brokerMergeCostPerRow
      val segmentOutputTransportCost = queryOutputSizeEstimate *
        (druidOutputTransportCostPerRowFactor * shuffleCostPerRow)
      val queryCost: Double = {
        /*
         * SearchQuerySpecs cannot be run against broker.
         */
        if ( classOf[SearchQuerySpec].isAssignableFrom(cI.querySpecClass)) {
          Double.MaxValue
        } else {
          numWaves * processingCostPerHist + segmentOutputTransportCost + brokertMergeCost
        }
      }

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
        numSegsPerQuery * segmentOutputSizeEstimate * histProcessingCostPerRow

      val histMergeCost : Double =
        (numSegsPerQuery - 1) * segmentOutputSizeEstimate * histMergeCostPerRow

      val segmentOutputTransportCost = estimateOutputSizePerHist *
        (druidOutputTransportCostPerRowFactor * shuffleCostPerRow)

      val shuffleCost = numWaves * segmentOutputSizeEstimate * shuffleCostPerRow

      val sparkSchedulingCost =
        numWaves * Math.min(parallelismPerWave, numSegmentsProcessed) * sparkSchedulingCostPerTask

      val sparkAggCost = numWaves * segmentOutputSizeEstimate *
        (sparkAggregationCostPerRowFactor * shuffleCostPerRow)

      val costPerHistoricalWave = processingCostPerHist + histMergeCost + segmentOutputTransportCost

      val druidStageCost = numWaves * costPerHistoricalWave

      val queryCost = druidStageCost + shuffleCost + sparkSchedulingCost + sparkAggCost

      HistoricalQueryCost(
        numWaves,
        numSegsPerQuery,
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
         |histProcessingCost = $histProcessingCostPerRow
         |queryOutputSizeEstimate = $queryOutputSizeEstimate
         |segmentOutputSizeEstimate = $segmentOutputSizeEstimate
         |numSegmentsProcessed = $numSegmentsProcessed
         |numSparkCores = $numSparkCores
         |numHistoricalThreads = $numHistoricalThreads
         |parallelismPerWave = $parallelismPerWave
         |
       """.stripMargin)

    val allCosts : ArrayBuffer[DruidQueryCost] = ArrayBuffer()
    var minCost : DruidQueryCost = brokerQueryCost
    allCosts += minCost
    var minNumSegmentsPerQuery = -1


    (1 to histSegsPerQueryLimit).foreach { (numSegsPerQuery : Int) =>
      val c = histQueryCost(numSegsPerQuery)
      allCosts += c
      if ( c < minCost ) {
        minCost = c
        minNumSegmentsPerQuery = numSegsPerQuery
      }
    }

    val costDetails = CostDetails(
      cI,
      histProcessingCostPerRow,
      queryOutputSizeEstimate,
      segmentOutputSizeEstimate,
      numSegmentsProcessed,
      numSparkCores,
      numHistoricalThreads,
      parallelismPerWave,
      minCost,
      allCosts.toList
    )

    log.info(costDetails.toString)

    DruidQueryMethod(minCost.isInstanceOf[HistoricalQueryCost],
      minNumSegmentsPerQuery, minCost, costDetails)

  }
  
  /**
    * Cardinality = product of dimension(ndv) * dimension(selectivity)
    * where selectivity:
    * - is only applied for equality and in predicates.
    * - a predicate on a non-gby dimension is assumed not changed the cardinality estmate.
    * - or and not predicates are also assumed not to reduce cardinality.
    *
    * @param qs
    * @param drInfo
    * @return
    */
  def estimateNDV(qs : QuerySpec,
                  drInfo : DruidRelationInfo) : Long = {

    val m : MMap[String, Long] = MHashMap()

    def applyFilterSelectivity(f : FilterSpec) : Unit = f match {
      case SelectorFilterSpec(_, dm, _) if m.contains(dm) => m(dm) = 1
      case ExtractionFilterSpec(_, dm, _, InExtractionFnSpec(_, LookUpMap(_, lm)))
        if m.contains(dm) => m(dm) = Math.min(m(dm), lm.size)
      case LogicalFilterSpec("and", sfs) => sfs.foreach(applyFilterSelectivity)
      case _ => ()
    }

    qs.dimensionSpecs.foreach { d =>
      m(d.dimension) =
        drInfo.druidDS.columns(d.dimension).cardinality
    }
    if ( qs.filter.isDefined) {
      applyFilterSelectivity(qs.filter.get)
    }
    m.values.product
  }

  private[druid] def computeMethod[T <: QuerySpec](
                   sqlContext : SQLContext,
                   druidDSIntervals : List[Interval],
                   druidDSFullName : DruidRelationName,
                   druidDSOptions : DruidRelationOptions,
                   dimsNDVEstimate : Long,
                   queryIntervalMillis : Long,
                   querySpecClass : Class[_ <: T]
                   ) : DruidQueryMethod = {

    val shuffleCostPerRow: Double = 1.0

    val histMergeFactor = sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_HIST_MERGE_COST)
    val histMergeCostPerRow = shuffleCostPerRow * histMergeFactor

    val histSegsPerQueryLimit =
      sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_HIST_SEGMENTS_PERQUERY_LIMIT)

    val queryIntervalRatioScaleFactor =
      sqlContext.getConf(DruidPlanner.DRUID_QUERY_COST_MODEL_INTERVAL_SCALING_NDV)

    val historicalTimeSeriesProcessingCostPerRow = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_HISTORICAL_TIMESERIES_PROCESSING_COST)

    val historicalGByProcessigCostPerRow = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_HISTORICAL_PROCESSING_COST)

    val sparkScheulingCostPerTask = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_SPARK_SCHEDULING_COST)

    val sparkAggregationCostPerRow = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_SPARK_AGGREGATING_COST)

    val druidOutputTransportCostPerRow = sqlContext.getConf(
      DruidPlanner.DRUID_QUERY_COST_MODEL_OUTPUT_TRANSPORT_COST)

    val indexIntervalMillis = intervalsMillis(druidDSIntervals)
    val segIntervalMillis = {
      val segIn = DruidMetadataCache.getDataSourceInfo(
        druidDSFullName, druidDSOptions)._1.segments.head._interval
      val segInMillis = intervalsMillis(List(segIn))
      /*
      * cannot have 1 segment's interval be greater than the entire index's interval.
      */
      if ( segInMillis > indexIntervalMillis) {
        log.info(s"Druid Index Interval ${indexIntervalMillis}")
        log.info(s"Druid Segment Interval ${segInMillis}")
      }
      Math.min(indexIntervalMillis, segInMillis)
    }

    val avgNumSegmentsPerSegInterval : Double = {
      val totalSegments = DruidMetadataCache.getDataSourceInfo(
        druidDSFullName, druidDSOptions)._1.segments.size
      val totalIntervals : Long = Math.round(indexIntervalMillis/segIntervalMillis)
      (totalSegments / totalIntervals)
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

    val numSegmentsProcessed: Long = Math.round(
      Math.max(Math.round(queryIntervalMillis / segIntervalMillis), 1L
      ) * avgNumSegmentsPerSegInterval
    )

    val sparkCoresPerExecutor =
      sqlContext.sparkContext.getConf.get(
        "spark.executor.cores",
        sqlContext.sparkContext.schedulerBackend.defaultParallelism().toString
      ).toLong

    val numSparkExecutors = sqlContext.sparkContext.getExecutorMemoryStatus.size

    val numProcessingThreadsPerHistorical: Long =
      druidDSOptions.
        numProcessingThreadsPerHistorical.map(_.toLong).getOrElse(sparkCoresPerExecutor)

    val numHistoricals = {
      DruidMetadataCache.getDruidClusterInfo(
        druidDSFullName, druidDSOptions).histServers.size
    }

    compute(
      CostInput(
        dimsNDVEstimate,
        shuffleCostPerRow,
        histMergeFactor,
        histSegsPerQueryLimit,
        queryIntervalRatioScaleFactor,
        historicalTimeSeriesProcessingCostPerRow,
        historicalGByProcessigCostPerRow,
        sparkScheulingCostPerTask,
        sparkAggregationCostPerRow,
        druidOutputTransportCostPerRow,
        indexIntervalMillis,
        queryIntervalMillis,
        segIntervalMillis,
        numSegmentsProcessed,
        sparkCoresPerExecutor,
        numSparkExecutors,
        numProcessingThreadsPerHistorical,
        numHistoricals,
        querySpecClass
      )
    )
  }

  def computeMethod[T <: QuerySpec](sqlContext : SQLContext,
                                     drInfo : DruidRelationInfo,
                                     querySpec : T
                                   ) : DruidQueryMethod = {
    val queryIntervalMillis : Long = intervalsMillis(querySpec.intervalList.map(Interval.parse(_)))

    computeMethod(
      sqlContext,
      drInfo.druidDS.intervals,
      drInfo.fullName,
      drInfo.options,
      estimateNDV(querySpec, drInfo),
      queryIntervalMillis,
      querySpec.getClass
    )

  }

  def computeMethod[T <: QuerySpec](sqlContext : SQLContext,
                                    druidDSIntervals : List[Interval],
                                    druidDSFullName : DruidRelationName,
                                    druidDSOptions : DruidRelationOptions,
                                    ndvEstimate : Long,
                                    querySpec : T
                                   ) : DruidQueryMethod = {
    val queryIntervalMillis : Long = intervalsMillis(querySpec.intervalList.map(Interval.parse(_)))

    computeMethod(
      sqlContext,
      druidDSIntervals,
      druidDSFullName,
      druidDSOptions,
      ndvEstimate,
      queryIntervalMillis,
      querySpec.getClass
    )

  }
}
