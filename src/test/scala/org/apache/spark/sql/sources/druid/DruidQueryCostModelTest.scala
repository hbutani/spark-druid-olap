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
import org.joda.time.{DateTime, Period}
import org.scalatest.{BeforeAndAfterAll, fixture}
import org.sparklinedata.druid.{GroupByQuerySpec, QuerySpec}

import scala.language.implicitConversions

class DruidQueryCostModelTest extends fixture.FunSuite with
  fixture.TestDataFixture with BeforeAndAfterAll with Logging {

  implicit def toMillis(p : Period) : Long = {
    val s = new DateTime()
    val e = s.plus(p)
    (e.getMillis - s.getMillis)
  }

  val default_histMergeFactor : Double= 0.2
  val default_histSegsPerQueryLimit : Int = 3
  val default_brokerSizeThreshold : Int = 500
  val default_queryIntervalRatioScaleFactor : Double = 3.0
  val default_historicalTimeSeriesProcessingCost : Double = 0.1
  val default_historicalGByProcessigCost : Double = 0.25
  val default_sparkScheulingCostPerTask : Double = 1.0
  val default_sparkAggregationCostPerByte : Double = 0.15
  val default_druidOutputTransportCostPerByte : Double = 0.4

  val tpch_indexIntervalMillis : Long = Period.years(7)
  val one_year_intervalMillis : Long= Period.years(1)
  val one_month_intervalMillis : Long= Period.months(1)
  val one_day_intervalMillis : Long= Period.days(1)
  val half_day_intervalMillis : Long= Period.hours(12)
  val six_hours_intervalMillis : Long= Period.hours(6)
  val one_hour_intervalMillis : Long= Period.hours(1)

  val tpch_sparkCoresPerExecutor = 7
  val tpch_numSparkExecutors = 8
  val tpch_numProcessingThreadsPerHistorical = 7
  val tpch_numHistoricals = 8

  test("tpch_one_year_query") { td =>

    // distinct values estimate = 100
    var costScenario = CostInput(
      100,
      1.0,
      default_histMergeFactor,
      default_histSegsPerQueryLimit,
      default_queryIntervalRatioScaleFactor,
      default_historicalTimeSeriesProcessingCost,
      default_historicalGByProcessigCost,
      default_sparkScheulingCostPerTask,
      default_sparkAggregationCostPerByte,
      default_druidOutputTransportCostPerByte,
      tpch_indexIntervalMillis,
      one_year_intervalMillis,
      one_month_intervalMillis,
      Math.max(Math.round(one_year_intervalMillis / tpch_indexIntervalMillis), 1L),
      tpch_sparkCoresPerExecutor,
      tpch_numSparkExecutors,
      tpch_numProcessingThreadsPerHistorical,
      tpch_numHistoricals,
      classOf[GroupByQuerySpec]
    )
    DruidQueryCostModel.compute(costScenario)

    // distinct values estimate = 1000
    DruidQueryCostModel.compute(costScenario.copy(dimsNDVEstimate = 1000))

    // distinct values estimate = 10000
    DruidQueryCostModel.compute(costScenario.copy(dimsNDVEstimate = 10000))
  }


  test("tpch_small_result_query") { td =>


    val costScenario = CostInput(
      100,
      1.0,
      default_histMergeFactor,
      default_histSegsPerQueryLimit,
      default_queryIntervalRatioScaleFactor,
      default_historicalTimeSeriesProcessingCost,
      default_historicalGByProcessigCost,
      default_sparkScheulingCostPerTask,
      default_sparkAggregationCostPerByte,
      default_druidOutputTransportCostPerByte,
      tpch_indexIntervalMillis,
      one_year_intervalMillis,
      one_month_intervalMillis,
      Math.max(Math.round(one_year_intervalMillis / tpch_indexIntervalMillis), 1L),
      tpch_sparkCoresPerExecutor,
      tpch_numSparkExecutors,
      tpch_numProcessingThreadsPerHistorical,
      tpch_numHistoricals,
      classOf[GroupByQuerySpec]
    )
    DruidQueryCostModel.compute(costScenario)
  }


  test("tpch_fullindextime__query") { td =>

    // distinct values estimate = 100
    val costScenario = CostInput(
      10000,
      1.0,
      default_histMergeFactor,
      default_histSegsPerQueryLimit,
      default_queryIntervalRatioScaleFactor,
      default_historicalTimeSeriesProcessingCost,
      default_historicalGByProcessigCost,
      default_sparkScheulingCostPerTask,
      default_sparkAggregationCostPerByte,
      default_druidOutputTransportCostPerByte,
      tpch_indexIntervalMillis,
      tpch_indexIntervalMillis,
      one_month_intervalMillis,
      Math.max(Math.round(tpch_indexIntervalMillis / tpch_indexIntervalMillis), 1L),
      tpch_sparkCoresPerExecutor,
      tpch_numSparkExecutors,
      tpch_numProcessingThreadsPerHistorical,
      tpch_numHistoricals,
      classOf[GroupByQuerySpec]
    )
    DruidQueryCostModel.compute(costScenario)

    // distinct values estimate = 1000
    DruidQueryCostModel.compute(costScenario.copy(dimsNDVEstimate = 1000))

    // distinct values estimate = 10000
    DruidQueryCostModel.compute(costScenario.copy(dimsNDVEstimate = 10000))
  }


}
