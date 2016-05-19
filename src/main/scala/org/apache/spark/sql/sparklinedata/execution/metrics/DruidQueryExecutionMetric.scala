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

package org.apache.spark.sql.sparklinedata.execution.metrics

import org.apache.spark.{Accumulable, AccumulableParam}
import org.sparklinedata.druid.metadata.{DruidQueryExecutionView, DruidQueryHistory}


class DruidQueryExecutionMetricParam extends AccumulableParam[String, DruidQueryExecutionView] {

  override def addAccumulator(r: String, t: DruidQueryExecutionView): String = {
    DruidQueryHistory.add(t)
    r
  }

  override def addInPlace(r1: String, r2: String): String = {
    r1
  }

  override def zero(initialValue: String): String = {
    initialValue
  }
}

class DruidQueryExecutionMetric extends
  Accumulable[String, DruidQueryExecutionView]("",
    new DruidQueryExecutionMetricParam(),
    None,
    true) {

}
