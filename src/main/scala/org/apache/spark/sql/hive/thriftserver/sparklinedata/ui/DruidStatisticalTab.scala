package org.apache.spark.sql.hive.thriftserver.sparklinedata.ui

import org.apache.spark.sql.hive.thriftserver.sparklinedata.ui.DruidStatisticalTab._
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{Logging, SparkContext, SparkException}


private[thriftserver] class DruidStatisticalTab(sparkContext: SparkContext)
  extends SparkUITab(getSparkUI(sparkContext), "druid") with Logging {

  override val name = "DruidQuery Statistics"
  val parent = getSparkUI(sparkContext)
  // val listener = HiveThriftServer2.sparkline_listenerD
  attachPage(new DruidStatisticalPage(this))
  parent.attachTab(this)
  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

private[spark] object DruidStatisticalTab {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}