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

case class CostDetails(costInput : CostInput,
                       histMergeCostPerRow: Double,
                       brokerMergeCostPerRow: Double,
                       histInputProcessingCostPerRow: Double,
                       histOutputProcessingCostPerRow: Double,
                       queryInputSizeEstimate: Long,
                       segmentInputSizeEstimate : Long,
                       queryOutputSizeEstimate: Long,
                       segmentOutputSizeEstimate: Long,
                       numSparkCores: Long,
                       numHistoricalThreads: Long,
                       parallelismPerWave: Long,
                       minCost : DruidQueryCost,
                       allCosts : List[DruidQueryCost]
                      ) {


  override def toString : String = {
    val header = s"""Druid Query Cost Model::
                     ${costInput}
Cost Per Row(
             histMergeCost=$histMergeCostPerRow,
             brokerMergeCost=$brokerMergeCostPerRow,
             histInputProcessingCost=$histInputProcessingCostPerRow,
             histOutputProcessingCost=$histOutputProcessingCostPerRow
            )
numSegmentsProcessed = ${costInput.numSegmentsProcessed}
Size Estimates(
               queryInputSize=$queryInputSizeEstimate,
               segmentInputSize=$segmentInputSizeEstimate,
               queryOutputSize=$queryOutputSizeEstimate,
               segmentOutputSize=$segmentOutputSizeEstimate
              )
Environment(
            numSparkCores = $numSparkCores
            numHistoricalThreads = $numHistoricalThreads
            parallelismPerWave = $parallelismPerWave
           )
                     |minCost : $minCost
                     |
                     |""".stripMargin


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

case class CostInput(
                                    inputEstimate : Long,
                                    outputEstimate : Long,
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
                      querySpec : Either[QuerySpec,Class[_ <: QuerySpec]]
                    ) {

  // scalastyle:off line.size.limit
  override def toString : String = {
    s"""
       |inputEstimate = $inputEstimate
       |outputEstimate = $outputEstimate
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
       |querySpec = $querySpec
     """.stripMargin
  }
  // scalastyle:on
}

sealed trait QueryCost extends Logging {
  val cI: CostInput

  import cI._

  def histMergeCostPerRow: Double

  def brokerMergeCostPerRow: Double

  def histInputProcessingCostPerRow: Double
  def histOutputProcessingCostPerRow: Double

  def queryInputSizeEstimate: Long

  def segmentInputSizeEstimate : Long

  def queryOutputSizeEstimate: Long

  def segmentOutputSizeEstimate: Long

  def numSparkCores: Long

  def numHistoricalThreads: Long

  def parallelismPerWave: Long

  def estimateNumWaves(numSegsPerQuery: Long,
                       parallelism: Long = parallelismPerWave): Long = {
    var d = numSegmentsProcessed.toDouble / numSegsPerQuery.toDouble
    d = d / parallelism
    Math.round(d + 0.5)
  }

  def shuffleCostPerWave: Double

  def sparkSchedulingCostPerWave: Double

  def sparkAggCostPerWave: Double

  def intervalRatio(intervalMillis: Long): Double =
    Math.min(intervalMillis.toDouble / indexIntervalMillis.toDouble, 1.0)

  def scaledRatio(intervalMillis: Long): Double = {
    val s = Math.min(queryIntervalRatioScaleFactor * intervalRatio(intervalMillis) * 10.0, 10.0)

    if (s < 1.0) {
      queryIntervalRatioScaleFactor * intervalRatio(intervalMillis)
    } else {
      Math.log10(s)
    }
  }

  def estimateInput(intervalMillis : Long) : Long

  def estimateOutput(intervalMillis : Long) : Long

  def brokerQueryCost : DruidQueryCost

  def histQueryCost(numSegsPerQuery : Long) : DruidQueryCost

  def druidQueryMethod: DruidQueryMethod = {

    log.info(
      s"""Druid Query Cost Model Input:
          |$cI
          |Cost Per Row(
          |             histMergeCost=$histMergeCostPerRow,
          |             brokerMergeCost=$brokerMergeCostPerRow,
          |             histInputProcessingCost=$histInputProcessingCostPerRow,
          |             histOutputProcessingCost=$histOutputProcessingCostPerRow
          |            )
          |numSegmentsProcessed = $numSegmentsProcessed
          |Size Estimates(
          |               queryInputSize=$queryInputSizeEstimate,
          |               segmentInputSize=$segmentInputSizeEstimate,
          |               queryOutputSize=$queryOutputSizeEstimate,
          |               segmentOutputSize=$segmentOutputSizeEstimate
          |              )
          |Environment(
          |            numSparkCores = $numSparkCores
          |            numHistoricalThreads = $numHistoricalThreads
          |            parallelismPerWave = $parallelismPerWave
          |           )
          |
       """.stripMargin)

    val allCosts: ArrayBuffer
      [DruidQueryCost] =
      ArrayBuffer()
    var minCost:


    DruidQueryCost = brokerQueryCost
    allCosts += minCost
    var
    minNumSegmentsPerQuery = -1


    (1 to
      histSegsPerQueryLimit).foreach { (numSegsPerQuery: Int) =>
      val c = histQueryCost(numSegsPerQuery)
      allCosts += c
      if (c < minCost) {
        minCost = c
        minNumSegmentsPerQuery =
          numSegsPerQuery
      }
    }

    val costDetails = CostDetails(
      cI,
      histMergeCostPerRow,
      brokerMergeCostPerRow,
      histInputProcessingCostPerRow,
      histOutputProcessingCostPerRow,
      queryInputSizeEstimate,
      segmentInputSizeEstimate,
      queryOutputSizeEstimate,
      segmentOutputSizeEstimate,
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

}

class AggQueryCost(val cI : CostInput) extends QueryCost {

  import cI._

  val histMergeCostPerRow = shuffleCostPerRow * histMergeCostPerRowFactor
  val brokerMergeCostPerRow = shuffleCostPerRow * histMergeCostPerRowFactor

  val histInputProcessingCostPerRow =
    historicalTimeSeriesProcessingCostPerRowFactor * shuffleCostPerRow

  val histOutputProcessingCostPerRow =
    historicalGByProcessigCostPerRowFactor * shuffleCostPerRow

  val queryInputSizeEstimate = estimateInput(queryIntervalMillis)

  val segmentInputSizeEstimate : Long = estimateInput(segIntervalMillis)

  val queryOutputSizeEstimate = estimateOutput(queryIntervalMillis)

  val segmentOutputSizeEstimate : Long = estimateOutput(segIntervalMillis)

  val numSparkCores : Long = {
    numSparkExecutors * sparkCoresPerExecutor
  }

  val numHistoricalThreads = numHistoricals * numProcessingThreadsPerHistorical

  val parallelismPerWave = Math.min(numHistoricalThreads, numSparkCores)

  def estimateOutput(intervalMillis : Long) : Long =
    Math.round(outputEstimate * scaledRatio(intervalMillis))

  def estimateInput(intervalMillis : Long) : Long =
    Math.round(inputEstimate * intervalRatio(intervalMillis))


  def shuffleCostPerWave = segmentOutputSizeEstimate * shuffleCostPerRow

  def sparkSchedulingCostPerWave =
    Math.min(parallelismPerWave, numSegmentsProcessed) * sparkSchedulingCostPerTask

  def sparkAggCostPerWave = segmentOutputSizeEstimate *
    (sparkAggregationCostPerRowFactor * shuffleCostPerRow)

  def costPerHistorical(numSegsPerQuery : Int) : Double = {
    val inputProcessingCostPerHist: Double =
      numSegsPerQuery * segmentInputSizeEstimate * histInputProcessingCostPerRow

    val outputProcessingCostPerHist: Double =
      numSegsPerQuery * segmentOutputSizeEstimate * histOutputProcessingCostPerRow

    inputProcessingCostPerHist + outputProcessingCostPerHist
  }

  def brokerMergeCost : Double = {
    val numMerges = 2 * (numSegmentsProcessed - 1)

    (Math.max(numMerges.toDouble / numProcessingThreadsPerHistorical.toDouble,1.0))  *
        segmentOutputSizeEstimate * brokerMergeCostPerRow
  }

  def brokerQueryCost : DruidQueryCost = {
    val numWaves: Long = estimateNumWaves(1, numHistoricalThreads)
    val processingCostPerHistPerWave : Double = costPerHistorical(1)

    val brokerTransportCostPerWave = segmentOutputSizeEstimate *
      (druidOutputTransportCostPerRowFactor * shuffleCostPerRow)

    val histCostPerWave = processingCostPerHistPerWave + brokerTransportCostPerWave

    val mergeCost : Double = brokerMergeCost

    val segmentOutputTransportCost = queryOutputSizeEstimate *
      (druidOutputTransportCostPerRowFactor * shuffleCostPerRow)


    val queryCost: Double =
      numWaves * histCostPerWave + segmentOutputTransportCost + mergeCost

    BrokerQueryCost(
      numWaves,
      numWaves * histCostPerWave,
      mergeCost,
      segmentOutputTransportCost,
      queryCost
    )
  }

  def histQueryCost(numSegsPerQuery : Long) : DruidQueryCost = {

    val numWaves = estimateNumWaves(numSegsPerQuery)
    val estimateOutputSizePerHist =
      estimateOutput(segIntervalMillis * numSegsPerQuery)

    val inputProcessingCostPerHist : Double =
      numSegsPerQuery * segmentInputSizeEstimate * histInputProcessingCostPerRow

    val outputProcessingCostPerHist : Double =
      numSegsPerQuery * segmentOutputSizeEstimate * histOutputProcessingCostPerRow

    val processingCostPerHist = inputProcessingCostPerHist + outputProcessingCostPerHist

    val histMergeCost : Double =
      (numSegsPerQuery - 1) * segmentOutputSizeEstimate * histMergeCostPerRow

    val segmentOutputTransportCost = estimateOutputSizePerHist *
      (druidOutputTransportCostPerRowFactor * shuffleCostPerRow)

    val costPerHistoricalWave = processingCostPerHist + histMergeCost + segmentOutputTransportCost
    val costPerSparkWave = shuffleCostPerWave + sparkSchedulingCostPerWave + sparkAggCostPerWave

    val druidStageCost = numWaves * costPerHistoricalWave
    val sparkStageCost = numWaves * costPerSparkWave

    val queryCost = druidStageCost + sparkStageCost

    HistoricalQueryCost(
      numWaves,
      numSegsPerQuery,
      estimateOutputSizePerHist,
      processingCostPerHist,
      histMergeCost,
      segmentOutputTransportCost,
      shuffleCostPerWave,
      sparkSchedulingCostPerWave,
      sparkAggCostPerWave,
      costPerHistoricalWave,
      druidStageCost,
      queryCost
    )
  }

}

class GroupByQueryCost(cI : CostInput) extends AggQueryCost(cI) {

}

/*
 * - OutputSize = InputSize
 * - there is no cost for doing GroupBy.
 * - Prevent Broker plans. TODO revisit this
 * - No Spark-side shuffle.
 */
class SelectQueryCost(i : CostInput) extends GroupByQueryCost(i) {

  override val histMergeCostPerRow = 0.0
  override val brokerMergeCostPerRow = 0.0

  override val histOutputProcessingCostPerRow = 0.0

  override def estimateOutput(intervalMillis : Long) = estimateInput(intervalMillis)

  override def shuffleCostPerWave = 0.0

  override def sparkAggCostPerWave = 0.0

  override def brokerMergeCost : Double = Double.MaxValue
}

/*
 * - inputSize = 0, because rows are not scanned.
 * - Prevent Broker plans.
 */
class SearchQueryCost(i : CostInput) extends GroupByQueryCost(i) {
  override val queryInputSizeEstimate : Long = 0

  override val segmentInputSizeEstimate : Long = 0

  override def brokerMergeCost : Double = Double.MaxValue

}

/*
 * - outputSize = 1
 */
class TimeSeriesQueryCost(i : CostInput) extends GroupByQueryCost(i) {

  override val queryOutputSizeEstimate : Long = 1

  override val segmentOutputSizeEstimate : Long = 1
}

/*
 * - queryOutputSize = query TopN threshold
 * - segmentOutputSize = queryContext maxTopNThreshold
 */
class TopNQueryCost(i : CostInput) extends GroupByQueryCost(i) {
  import cI._
  override val segmentOutputSizeEstimate : Long = cI.querySpec match {
    case Left(qS : TopNQuerySpec) if qS.context.flatMap(_.minTopNThreshold).isDefined =>
      qS.context.flatMap(_.minTopNThreshold).get
    case _ => estimateOutput(segIntervalMillis)
  }

  override val queryOutputSizeEstimate: Long = cI.querySpec match {
    case Left(qS : TopNQuerySpec) => qS.threshold
    case _ => estimateOutput(queryIntervalMillis)
  }
}

object DruidQueryCostModel extends Logging {

  def intervalsMillis(intervals : List[Interval]) : Long = Utils.intervalsMillis(intervals)

  def intervalNDVEstimate(
                         intervalMillis : Long,
                         totalIntervalMillis : Long,
                         ndvForIndexEstimate : Long,
                         queryIntervalRatioScaleFactor : Double
                         ) : Long = {
    val intervalRatio : Double = Math.min(intervalMillis.toDouble/totalIntervalMillis.toDouble, 1.0)
    var scaledRatio = Math.min(queryIntervalRatioScaleFactor * intervalRatio * 10.0, 10.0)
    if ( scaledRatio < 1.0 ) {
      scaledRatio = queryIntervalRatioScaleFactor * intervalRatio
    } else {
      scaledRatio = Math.log10(scaledRatio)
    }
    Math.round(ndvForIndexEstimate * scaledRatio)
  }

  private[druid] def compute[T <: QuerySpec](cI : CostInput) : DruidQueryMethod = {
    val queryCost = cI.querySpec match {
      case Left(q : SelectSpec) => new SelectQueryCost(cI)
      case Right(cls) if classOf[SelectSpec].isAssignableFrom(cls) =>
        new SelectQueryCost(cI)
      case Left(q : SearchQuerySpec) => new SearchQueryCost(cI)
      case Right(cls) if classOf[SearchQuerySpec].isAssignableFrom(cls) =>
        new SearchQueryCost(cI)
      case Left(q : TimeSeriesQuerySpec) => new TimeSeriesQueryCost(cI)
      case Right(cls) if classOf[TimeSeriesQuerySpec].isAssignableFrom(cls) =>
        new TimeSeriesQueryCost(cI)
      case Left(q : TopNQuerySpec) => new TopNQueryCost(cI)
      case Right(cls) if classOf[TopNQuerySpec].isAssignableFrom(cls) =>
        new TopNQueryCost(cI)
      case Left(q : GroupByQuerySpec) => new GroupByQueryCost(cI)
       case Right(cls) if classOf[GroupByQuerySpec].isAssignableFrom(cls) =>
         new GroupByQueryCost(cI)
      case _ => new GroupByQueryCost(cI)
    }
    queryCost.druidQueryMethod
  }

  def estimateInput(qs : QuerySpec,
                                  drInfo : DruidRelationInfo) : Long = {

    def applyFilterSelectivity(f : FilterSpec) : Double = f match {
      case SelectorFilterSpec(_, dm, _) =>
        (1.0/drInfo.druidDS.columns(dm).cardinality)
      case ExtractionFilterSpec(_, dm, _, InExtractionFnSpec(_, LookUpMap(_, lm)))
      =>
        (lm.size.toDouble/drInfo.druidDS.columns(dm).cardinality)
      case LogicalFilterSpec("and", sfs) => sfs.map(applyFilterSelectivity).product
      case LogicalFilterSpec("or", sfs) => sfs.map(applyFilterSelectivity).sum
      case NotFilterSpec(_, fs) => 1.0 - applyFilterSelectivity(fs)
      case _ => 1.0/3.0
    }

    val selectivity : Double = qs.filter.map(applyFilterSelectivity).getOrElse(1.0)
    (drInfo.druidDS.numRows.toDouble * selectivity).toLong
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
  private def estimateOutputCardinality(qs : QuerySpec,
                  drInfo : DruidRelationInfo) : Long = {

    val gByColumns : Map[String, DruidColumn] =
      qs.dimensionSpecs.map(ds => (ds.dimension, drInfo.druidDS.columns(ds.dimension))).toMap

    var queryNumRows = gByColumns.values.map(_.cardinality).map(_.toDouble).product
    queryNumRows = Math.min(queryNumRows, drInfo.druidDS.numRows)

    def applyFilterSelectivity(f : FilterSpec) : Unit = f match {
      case SelectorFilterSpec(_, dm, _) if gByColumns.contains(dm) => {
        queryNumRows = queryNumRows * (1.0/gByColumns(dm).cardinality)
      }
      case ExtractionFilterSpec(_, dm, _, InExtractionFnSpec(_, LookUpMap(_, lm)))
        if gByColumns.contains(dm) => {
        queryNumRows = queryNumRows * (lm.size.toDouble/gByColumns(dm).cardinality)
      }
      case LogicalFilterSpec("and", sfs) => sfs.foreach(applyFilterSelectivity)
      case _ => ()
    }

    if ( qs.filter.isDefined) {
      applyFilterSelectivity(qs.filter.get)
    }
    queryNumRows.toLong
  }

  def estimateOutput(qs : QuerySpec,
                  drInfo : DruidRelationInfo) : Long = qs match {
    case select : SelectSpec => estimateInput(select, drInfo)
    case _ => estimateOutputCardinality(qs, drInfo)
  }

  private[druid] def computeMethod(
                   sqlContext : SQLContext,
                   druidDSIntervals : List[Interval],
                   druidDSFullName : DruidRelationName,
                   druidDSOptions : DruidRelationOptions,
                   inputEstimate : Long,
                   outputEstimate : Long,
                   queryIntervalMillis : Long,
                   querySpec : QuerySpec
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
        inputEstimate,
        outputEstimate,
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
        Left(querySpec)
      )
    )
  }

  def computeMethod(sqlContext : SQLContext,
                                     drInfo : DruidRelationInfo,
                                     querySpec : QuerySpec
                                   ) : DruidQueryMethod = {
    val queryIntervalMillis : Long = intervalsMillis(querySpec.intervalList.map(Interval.parse(_)))

    computeMethod(
      sqlContext,
      drInfo.druidDS.intervals,
      drInfo.fullName,
      drInfo.options,
      estimateInput(querySpec, drInfo),
      estimateOutput(querySpec, drInfo),
      queryIntervalMillis,
      querySpec
    )

  }

  def computeMethod(sqlContext : SQLContext,
                                    druidDSIntervals : List[Interval],
                                    druidDSFullName : DruidRelationName,
                                    druidDSOptions : DruidRelationOptions,
                                    inputEstimate: Long,
                                    outputEstimate : Long,
                                    querySpec : QuerySpec
                                   ) : DruidQueryMethod = {
    val queryIntervalMillis : Long = intervalsMillis(querySpec.intervalList.map(Interval.parse(_)))

    computeMethod(
      sqlContext,
      druidDSIntervals,
      druidDSFullName,
      druidDSOptions,
      inputEstimate,
      outputEstimate,
      queryIntervalMillis,
      querySpec
    )

  }
}
