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

package org.apache.spark.sql.hive.thriftserver.sparklinedata.ui

import org.apache.spark.sql.hive.thriftserver.sparklinedata.ui.DruidQueriesTab._
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.SPLLogging

private[thriftserver] class DruidQueriesTab(sparkContext: SparkContext)
  extends SparkUITab(getSparkUI(sparkContext), "druid") with SPLLogging {

  override val name = "Druid Query Details"
  val parent = getSparkUI(sparkContext)
  attachPage(new DruidQueriesPage(this))
  parent.attachTab(this)
  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

private[spark] object DruidQueriesTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}