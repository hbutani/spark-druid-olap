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

import java.util.TimeZone

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.scalatest.BeforeAndAfterEach
import org.sparklinedata.druid._
import org.sparklinedata.druid.client.BaseTest
import org.sparklinedata.druid.metadata.DruidRelationInfo

trait PlanningTestHelper extends PredicateHelper {
  System.setProperty("user.timezone", "UTC")
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  override def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    super.splitConjunctivePredicates(condition)
  }
}

abstract class PlanningTest extends BaseTest with BeforeAndAfterEach with PlanningTestHelper {

  val dPlanner = new DruidPlanner(TestHive)
  var tab: DataFrame = _
  var drInfo: DruidRelationInfo = _
  var dqb: DruidQueryBuilder = _
  var iCE: IntervalConditionExtractor = _
  var iCE2: SparkIntervalConditionExtractor = _

  override def beforeAll() = {
    super.beforeAll()
    tab = TestHive.table("orderLineItemPartSupplier")
    drInfo = tab.queryExecution.optimizedPlan.
      asInstanceOf[LogicalRelation].relation.asInstanceOf[DruidRelation].info
  }

  override protected def beforeEach(): Unit = {
    dqb = DruidQueryBuilder(drInfo)
    iCE = new IntervalConditionExtractor(dqb)
    iCE2 = new SparkIntervalConditionExtractor(dqb)
  }

  def validateFilter(filterStr: String,
                     pushedToDruid: Boolean = true,
                     filSpec: Option[FilterSpec] = None,
                     intervals: List[Interval] = List()
                    ): Unit = {
    val q = tab.where(filterStr)
    val filter = q.queryExecution.optimizedPlan.asInstanceOf[Filter]
    val dqbs = dPlanner.translateProjectFilter(
      Some(dqb),
      Seq(),
      splitConjunctivePredicates(filter.condition),
      true
    )
    if (pushedToDruid) {
      assert(dqbs.size == 1)
      val odqb = dqbs(0)
      assert(odqb.filterSpec == filSpec)
      assert(odqb.queryIntervals.intervals == intervals)
    }
  }

}
