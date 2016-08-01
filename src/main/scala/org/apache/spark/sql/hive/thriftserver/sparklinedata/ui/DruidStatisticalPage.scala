package org.apache.spark.sql.hive.thriftserver.sparklinedata.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.sparklinedata.druid.metadata.{DruidQueryExecutionView, DruidQueryHistory}
import scala.xml.Node


private[ui] class DruidStatisticalPage(parent: DruidStatisticalTab) extends WebUIPage("") with Logging {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content = generateDruidStatsTable()
    UIUtils.headerSparkPage("DruidQuery Statistics", content, parent, Some(5000))
  }

  private def generateDruidStatsTable(): Seq[Node] = {
    val numStatement = DruidQueryHistory.getHistory.size
    val table = if (numStatement > 0) {
      val headerRow = Seq("stageId", "partitionId", "taskAttemptId", "druidQueryServer",
        "druidSegIntervals", "startTime", "druidExecTime", "queryExecTime", "numRows",
        "druidQuery")
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
        </tr>
      }
      Some(UIUtils.listingTable(headerRow, generateDataRow,
        druidContent, false, None, Seq(null), false))
    } else {
      None
    }
    val content =
      <h5 id="sqlstat">Druid Query Statistics</h5> ++
        <div>
          <ul class="unstyled">
            {table.getOrElse("No statistics have been generated yet.")}
          </ul>
        </div>
    content
  }
}
