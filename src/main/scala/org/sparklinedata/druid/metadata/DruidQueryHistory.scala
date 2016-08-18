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

package org.sparklinedata.druid.metadata

import org.apache.spark.sql.hive.thriftserver.sparklinedata.HiveThriftServer2

import scala.collection.immutable.Queue
import scala.language.implicitConversions

case class DruidQueryExecutionView(
                         stageId : Int,
                         partitionId : Int,
                         taskAttemptId : Long,
                         druidQueryServer : String,
                         druidSegIntervals : Option[List[(String,String)]],
                         startTime : String,
                         druidExecTime : Long,
                         queryExecTime : Long,
                         numRows : Int,
                         druidQuery : String,
                         sqlStmt: Option[String] = None
                         )

object DruidQueryHistory {

  /*
   * from
   * http://stackoverflow.com/questions/6918731/maximum-length-for-scala-queue/6920366#6920366
   */

  lazy val maxSize = System.getProperty("sparkline.queryhistory.maxsize", "500").toInt


  class FiniteQueue[A](q: Queue[A]) {
    def enqueueFinite[B >: A](elem: B): Queue[B] = {
      var ret = q.enqueue(elem)
      while (ret.size > maxSize) { ret = ret.dequeue._2 }
      ret
    }
  }
  implicit def queue2finitequeue[A](q: Queue[A]) = new FiniteQueue[A](q)


  private var history : Queue[DruidQueryExecutionView] = Queue[DruidQueryExecutionView]()

  def add(dq : DruidQueryExecutionView) : Unit = synchronized {
    history = history.enqueueFinite(
      dq.copy(sqlStmt = HiveThriftServer2.getSqlStmt(dq.stageId))
    )
  }

  def getHistory : List[DruidQueryExecutionView] = synchronized {
    history.toList
  }
}
