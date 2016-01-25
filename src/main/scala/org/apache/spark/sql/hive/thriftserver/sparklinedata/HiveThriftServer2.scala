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

import org.apache.hive.service.server.HiveServerServerOptionsProcessor
import org.apache.spark.Logging
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.sql.hive.thriftserver.{HiveThriftServer2 => RealHiveThriftServer2 }

import org.sparklinedata.spark.dateTime.Functions
import org.apache.spark.sql.sources.druid.DruidPlanner

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
    SparkSQLEnv.init()

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

