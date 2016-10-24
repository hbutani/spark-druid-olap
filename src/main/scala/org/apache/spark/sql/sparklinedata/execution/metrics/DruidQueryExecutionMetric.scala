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

import java.util.{ArrayList, Collections}

import org.apache.spark.util.AccumulatorV2
import org.sparklinedata.druid.metadata.{DruidQueryExecutionView, DruidQueryHistory}


class DruidQueryExecutionMetric extends
  AccumulatorV2[DruidQueryExecutionView, java.util.List[DruidQueryExecutionView]] {

  import scala.collection.JavaConverters._

  private val _list: java.util.List[DruidQueryExecutionView] =
    Collections.synchronizedList(new ArrayList[DruidQueryExecutionView]())

  private def getList : java.util.List[DruidQueryExecutionView] = {
    if (isAtDriverSide) DruidQueryHistory.getHistory.asJava else _list
  }

  override def isZero: Boolean = {
    getList.isEmpty
  }

  override def copyAndReset() = new DruidQueryExecutionMetric

  override def copy(): DruidQueryExecutionMetric = {
    val newAcc = new DruidQueryExecutionMetric
    if (!isAtDriverSide) newAcc._list.addAll(_list)
    newAcc
  }

  override def reset(): Unit = {
    if (isAtDriverSide) DruidQueryHistory.clear else _list.clear()
  }

  override def add(v: DruidQueryExecutionView): Unit = {
    if (isAtDriverSide) DruidQueryHistory.add(v) else _list.add(v)
  }

  private def addAll(v: java.util.List[DruidQueryExecutionView]): Unit = {
   v.asScala.foreach(add(_))
  }

  override def merge(other:
                     AccumulatorV2[DruidQueryExecutionView,
                       java.util.List[DruidQueryExecutionView]]):
  Unit = other match {
    case o: DruidQueryExecutionMetric => {
      addAll(o.value)
    }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value = _list.synchronized {
    java.util.Collections.unmodifiableList(getList)
  }

  private[spark] def setValue(newValue: java.util.List[DruidQueryExecutionView]): Unit = {
    reset()
    addAll(newValue)
  }

}
