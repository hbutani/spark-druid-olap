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

import org.apache.hive.service.server.{HiveServer2, HiveServerServerOptionsProcessor}
import org.apache.spark.scheduler.{SparkListenerJobStart, StatsReportListener}
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.sparklinedata.SparklineDataContext
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv._
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.sparklinedata.ui.DruidQueriesTab
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.sql.hive.thriftserver.{SparkSQLCLIDriver, SparkSQLEnv, HiveThriftServer2 => RealHiveThriftServer2}
import org.apache.spark.sql.planner.logical.DruidLogicalOptimizer
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.sparklinedata.spark.dateTime.Functions

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A wrapper for spark's [[org.apache.spark.sql.hive.thriftserver.HiveThriftServer2]].
  * On start, registers Sparkline dateTime functions and DruidPlanner into the
  * [[org.apache.spark.sql.hive.HiveContext]].
  */
object HiveThriftServer2 extends Logging {
  def hs2Listener: HS2Listener =
    RealHiveThriftServer2.listener.asInstanceOf[HS2Listener]

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
      startWithContext(SparkSQLEnv.hiveContext)
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

  def startWithContext(sqlContext: HiveContext): Unit = {
    /*
     * RealHiveThriftServer2.startWithContext copied here, so we can register
     * our listener.
     */
    val server = new RealHiveThriftServer2(sqlContext)
    server.init(sqlContext.hiveconf)
    server.start()
    RealHiveThriftServer2.listener =
      new HS2Listener(server, sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(hs2Listener)
    RealHiveThriftServer2.uiTab =
      if (sqlContext.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
        Some(new DruidQueriesTab(sqlContext.sparkContext))
        Some(new ThriftServerTab(sqlContext.sparkContext))
      } else {
        None
      }
  }

  def getSqlStmt(stageId: Int): Option[String] = {
    if (hs2Listener != null) hs2Listener.getSqlStmt(stageId) else None
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
      hiveContext = new SparklineDataContext(sparkContext)

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

/**
  * A inner sparkListener called in sc.stop to clean up the HiveThriftServer2.
  * Override default Listener to maintain mapping between EID, GID & STGID.
  */
private[thriftserver] class HS2Listener(override val server: HiveServer2,
                                        override val conf: SQLConf)
  extends HiveThriftServer2Listener(server, conf) {
  private val execIdToGIDStmt = new mutable.LinkedHashMap[String, (String, String)]
  private val grpIdToExecId = new mutable.LinkedHashMap[String, String]
  private val execIdToStageIDs = new mutable.LinkedHashMap[String, Seq[Int]]
  private val stageIdToexecID = new mutable.LinkedHashMap[Int, String]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    super.onJobStart(jobStart)
    for {
      props <- Option(jobStart.properties)
      groupId <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
      eid <- grpIdToExecId.get(groupId)} {
      for (stgid <- jobStart.stageIds)
        stageIdToexecID += stgid -> eid
      execIdToStageIDs += eid -> jobStart.stageIds
    }
  }

  override def onStatementStart(
                                 id: String,
                                 sessionId: String,
                                 statement: String,
                                 groupId: String,
                                 userName: String = "UNKNOWN"): Unit = synchronized {
    super.onStatementStart(id, sessionId, statement, groupId, userName)
    execIdToGIDStmt += id ->(groupId, statement)
    grpIdToExecId += groupId -> id
  }

  override def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit = {
    synchronized {
      cleanupStatement(id)
      super.onStatementError(id, errorMessage, errorTrace)
    }
  }

  override def onStatementFinish(id: String): Unit = synchronized {
    cleanupStatement(id)
    super.onStatementFinish(id)
  }

  def cleanupStatement(id: String): Unit = synchronized {
    for (stgIds <- execIdToStageIDs.get(id)) {
      stageIdToexecID --= (stgIds)
    }
    execIdToStageIDs -= (id)
    for ((gid, _) <- execIdToGIDStmt.get(id)) {
      grpIdToExecId -= (gid)
    }
    execIdToGIDStmt -= (id)
  }

  def getSqlStmt(stgId: Int): Option[String] = synchronized {
    for (execId <- stageIdToexecID.get(stgId); (_, stmt) <- execIdToGIDStmt.get(execId)) yield
      stmt
  }
}