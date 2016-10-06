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

package org.sparklinedata.druid.client

import java.io.{File, FileInputStream}
import java.net.URLEncoder
import java.util.concurrent.Callable

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.sources.druid.JsonOperations
import org.joda.time.{DateTime, Interval}
import org.sparklinedata.druid.{RetryUtils, Utils}


object OverlordTaskStatus extends Enumeration {
  val RUNNING, SUCCESS, FAILED = Value
}

case class OverlordTaskResponseObject(val id: String,
                                      val createdTime: DateTime,
                                      val queueInsertionTime: DateTime,
                                      val status: OverlordTaskStatus.Value
                                     )

case class OverlordTaskStatusStatus(
                                     id: String,
                                     status: OverlordTaskStatus.Value,
                                     duration: Long
                                   )

case class OverlordTaskStatusResponse(
                                       task: String,
                                       status: OverlordTaskStatusStatus
                                     )

class DruidOverlordClient(host: String,
                          port: Int,
                          useSmile: Boolean = false) extends DruidClient(host, port, useSmile) {
  self =>

  @transient val urlPrefix = s"http://$host:$port/druid/indexer/v1"

  val jsonOps = new JsonOperations {
    override def useSmile: Boolean = self.useSmile
  }

  import Utils._
  import jsonOps.jsonMethods._

  def submitTask(taskFile: File): String = {
    submitTask(IOUtils.toString(new FileInputStream(taskFile)))
  }

  def submitTask(task: String): String = {
    val url = s"$urlPrefix/task"
    val payload = parse(task)
    val response = post(url, payload)
    val m = response.extract[Map[String, String]]
    val taskId: String = m("task").asInstanceOf[String]
    taskId
  }

  def getTaskStatus(taskID: String): OverlordTaskStatus.Value = {
    val url = s"$urlPrefix/task/${URLEncoder.encode(taskID, "UTF-8")}/status"
    val jV = get(url)
    val s = pretty(jV)
    val m = jV.extract[OverlordTaskStatusResponse]
    m.status.status
  }

  def getRunningTasks: List[OverlordTaskResponseObject] = getTasks("runningTasks")

  def getWaitingTasks: List[OverlordTaskResponseObject] = getTasks("waitingTasks")

  def getPendingTasks: List[OverlordTaskResponseObject] = getTasks("pendingTasks")

  private def getTasks(identifier: String): List[OverlordTaskResponseObject] = {
    val url = s"$urlPrefix/$identifier"
    val jV = get(url)
    jV.extract[List[OverlordTaskResponseObject]]
  }

  def shutDownTask(taskID: String): Map[String, String] = {
    val url = s"$urlPrefix/task/${URLEncoder.encode(taskID, "UTF-8")}/shutdown"
    val response = post(url, null)
    response.extract[Map[String, String]]
  }

  def waitUntilTaskCompletes(taskID: String) {
    waitUntilTaskCompletes(taskID, 5000, 50)
  }

  def waitUntilTaskCompletes(taskID: String, millisEach: Int, numTimes: Int): Unit = {
    RetryUtils.retryUntil(
      new Callable[Boolean]() {
        @throws[Exception]
        def call: Boolean = {
          val status: OverlordTaskStatus.Value = getTaskStatus(taskID)
          if (status eq OverlordTaskStatus.FAILED) {
            throw new IllegalStateException("Indexer task FAILED")
          }
          status eq OverlordTaskStatus.SUCCESS
        }
      },
      true,
      millisEach,
      numTimes,
      taskID
    )
  }

  def timeBoundary(dataSource: String): Interval = ???

}
