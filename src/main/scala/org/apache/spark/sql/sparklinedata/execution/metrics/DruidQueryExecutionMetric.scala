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

import org.apache.spark.{Accumulable, GrowableAccumulableParam}
import org.sparklinedata.druid.metadata.{DruidQueryExecutionView, DruidQueryHistory}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}


class DruidQueryExecutionMetricParam extends
  GrowableAccumulableParam[ArrayBuffer[DruidQueryExecutionView], DruidQueryExecutionView] {
}

class DruidQueryExecutionMetric extends
  Accumulable[ArrayBuffer[DruidQueryExecutionView], DruidQueryExecutionView](
    ArrayBuffer(),
    new DruidQueryExecutionMetricParam(),
    Some("druidQueryMetric"),
    true) {

  private def isInDriver : Boolean = {
    Try(value) match {
      case Success(v) => true
      case _ => false
    }
  }

  private def addTermToHistory(term: DruidQueryExecutionView) : Unit = {
    if ( isInDriver ) {
      DruidQueryHistory.add(term)
    }
  }

  override def += (term: DruidQueryExecutionView) {
    addTermToHistory(term)
    super.+=(term)
  }

  override def add(term: DruidQueryExecutionView) {
    addTermToHistory(term)
    super.add(term)
  }

  override def ++= (terms: ArrayBuffer[DruidQueryExecutionView]) {
    terms.foreach(addTermToHistory(_))
    super.++=(terms)
  }

  override def merge(terms: ArrayBuffer[DruidQueryExecutionView]) {
    terms.foreach(addTermToHistory(_))
    super.merge(terms)
  }

}
