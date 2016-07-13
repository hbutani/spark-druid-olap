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

package org.apache.spark.sql.hive.thriftserver.sparklinedata

import java.io.PrintStream
import scala.collection.JavaConverters._
import org.apache.hive.service.server.HiveServerServerOptionsProcessor
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.sparklinedata.SparklineDataContext
import org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver
import org.apache.spark.sql.hive.thriftserver.{SparkSQLEnv, HiveThriftServer2 => RealHiveThriftServer2}
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.sql.planner.logical.DruidLogicalOptimizer
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.sparklinedata.spark.dateTime.Functions

/**
  * A wrapper for spark's [[org.apache.spark.sql.hive.thriftserver.HiveThriftServer2]].
  * On start, registers Sparkline dateTime functions and DruidPlanner into the
  * [[org.apache.spark.sql.hive.HiveContext]].
  */
object HiveThriftServer2 extends Logging {

  def main(args: Array[String]) {
    val optionsProcessor = new HiveServerServerOptionsProcessor("HiveThriftServer2")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    logInfo("Starting SparkContext")
    SparklineSQLEnv.init()

    Functions.register(SparkSQLEnv.hiveContext)
    DruidPlanner(SparkSQLEnv.hiveContext)

    ShutdownHookManager.addShutdownHook { () =>
      SparkSQLEnv.stop()
      RealHiveThriftServer2.uiTab.foreach(_.detach())
    }

    try {
      RealHiveThriftServer2.startWithContext(SparkSQLEnv.hiveContext)

      if (SparkSQLEnv.sparkContext.stopped.get()) {
        logError("SparkContext has stopped even if HiveServer2 has started, so exit")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }

}

object SparklineSQLEnv extends Logging {
  logDebug("Initializing SparkSQLEnv")

  def init() {
    if (hiveContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      val maybeSerializer = sparkConf.getOption("spark.serializer")
      val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))
        .set(
          "spark.serializer",
          maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
        .set(
          "spark.kryo.referenceTracking",
          maybeKryoReferenceTracking.getOrElse("false"))

      sparkContext = new SparkContext(sparkConf)
      sparkContext.addSparkListener(new StatsReportListener())
      hiveContext = new SparklineDataContext(sparkContext, DruidLogicalOptimizer)

      hiveContext.metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      hiveContext.metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      hiveContext.metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))

      hiveContext.setConf("spark.sql.hive.version", HiveContext.hiveExecutionVersion)

      if (log.isDebugEnabled) {
        hiveContext.hiveconf.getAllProperties.asScala.toSeq.sorted.foreach { case (k, v) =>
          logDebug(s"HiveConf var: $k=$v")
        }
      }
    }
  }
}