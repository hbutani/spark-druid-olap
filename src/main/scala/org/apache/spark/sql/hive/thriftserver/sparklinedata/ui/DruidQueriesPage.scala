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

import javax.servlet.http.HttpServletRequest
import org.apache.spark.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.sparklinedata.druid.metadata.{DruidQueryExecutionView, DruidQueryHistory}
import scala.xml.Node


private[ui] class DruidQueriesPage(parent: DruidQueriesTab) extends WebUIPage("") with Logging {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content = generateDruidStatsTable()
    UIUtils.headerSparkPage("Druid Query Details", content, parent, Some(5000))
  }

  private def generateDruidStatsTable(): Seq[Node] = {
    val numStatement = DruidQueryHistory.getHistory.size
    val table = if (numStatement > 0) {
      val headerRow = Seq("stageId", "partitionId", "taskAttemptId", "druidQueryServer",
        "druidSegIntervals", "startTime", "druidExecTime", "queryExecTime", "numRows",
        "druidQuery", "sql")
      val druidContent = DruidQueryHistory.getHistory
      def generateDataRow(info: DruidQueryExecutionView): Seq[Node] = {
        var interval = ""
        for(temp <- info.druidSegIntervals){
          interval += temp
        }
        val stageLink = "%s/stages/stage?id=%s&attempt=0"
          .format(UIUtils.prependBaseUri(parent.basePath), info.stageId)
        <tr>
          <td><a href={stageLink}> {info.stageId} </a></td>
          <td>
            {info.partitionId}
          </td>
          <td>{info.taskAttemptId}</td>
          <td>{info.druidQueryServer}</td>
          <td>{interval}</td>
          <td>{info.startTime}</td>
          <td>{info.druidExecTime}</td>
          <td>{info.queryExecTime}</td>
          <td>{info.numRows}</td>
          <td>{info.druidQuery}</td>
          <td>{info.sqlStmt.getOrElse("none")}</td>
        </tr>
      }
      Some(UIUtils.listingTable(headerRow, generateDataRow,
        druidContent, false, None, Seq(null), false))
    } else {
      None
    }
    val content =
      <h5 id="sqlstat">Druid Query Details</h5> ++
        <div>
          <ul class="unstyled">
            {table.getOrElse("No queries have been executed yet.")}
          </ul>
        </div>
    content
  }
}
