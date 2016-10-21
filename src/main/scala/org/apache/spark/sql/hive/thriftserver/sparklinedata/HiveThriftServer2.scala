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

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.scheduler.{SparkListenerJobStart, StatsReportListener}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.{log => _, logDebug => _, _}
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv._
import org.apache.spark.sql.hive.thriftserver.sparklinedata.ui.DruidQueriesTab
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.sql.hive.thriftserver.{SparkSQLCLIDriver, SparkSQLEnv, HiveThriftServer2 => RealHiveThriftServer2}
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SPLLogging
import org.apache.spark.sql.hive.sparklinedata.StuffReflect

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A wrapper for spark's [[org.apache.spark.sql.hive.thriftserver.HiveThriftServer2]].
  * On start, registers Sparkline dateTime functions and DruidPlanner into the
  * [[org.apache.spark.sql.hive.HiveContext]].
  */
object HiveThriftServer2 extends SPLLogging {
  def hs2Listener: HS2Listener =
    RealHiveThriftServer2.listener.asInstanceOf[HS2Listener]

  def hs2 : RealHiveThriftServer2 =
    RealHiveThriftServer2.listener.server.asInstanceOf[RealHiveThriftServer2]

  def main(args: Array[String]) {
    StuffReflect.changeSessionStateClass

    RealHiveThriftServer2.main(args)
    // FIXME above causes wrong listener to be registered.

    RealHiveThriftServer2.listener =
      new HS2Listener(hs2, sqlContext.conf)

    if (hs2.getHiveConf.getBoolVar(
      ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      invoke(classOf[HiveServer2], hs2, "addServerInstanceToZooKeeper",
        classOf[HiveConf] -> hs2.getHiveConf)
      ShutdownHookManager.addShutdownHook { () =>
        invoke(classOf[HiveServer2], hs2, "removeServerInstanceFromZooKeeper")
      }
    }

    if (SparkSQLEnv.sparkContext.getConf.getBoolean("spark.ui.enabled", true)) {
      if (DruidPlanner.getConfValue(sqlContext, DruidPlanner.DRUID_RECORD_QUERY_EXECUTION)) {
        Some(new DruidQueriesTab(sqlContext.sparkContext))
      }
    }

  }

  def getSqlStmt(stageId: Int): Option[String] = {
    if (hs2Listener != null) hs2Listener.getSqlStmt(stageId) else None
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