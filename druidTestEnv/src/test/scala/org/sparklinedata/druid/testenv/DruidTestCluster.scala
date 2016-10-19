/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.sparklinedata.druid.testenv

import java.io.{File, InputStreamReader}
import java.net.BindException
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.google.common.base.{Charsets, Joiner}
import com.google.common.io.{ByteStreams, CharStreams, Files}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.control._
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingCluster

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.io.Source
import scala.util.Try

class DruidTestDaemon(val serviceName: String,
                      val process: Process) {

  val logFile = new File(s"${DruidTestCluster.DRUID_RUNTIME_LOG}/$serviceName.stdout")
  val logStream = FileUtils.openOutputStream(logFile)
}

trait TestUtils {

  def classPathURLs(cl: ClassLoader =
                    Thread.currentThread().getContextClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ classPathURLs(cl.getParent)
    case _ => classPathURLs(cl.getParent)
  }

  def tempFolder: File = {
    val tempDir = Files.createTempDir()
    tempDir.deleteOnExit()
    tempDir
  }

  def writeResource(resource: String,
                    replacements: Map[String, String],
                    outputFolder: File): File = {
    val outName = resource.substring(DruidTestCluster.DRUID_RUNTIME_ENV_PACKAGE.length + 1)
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    val text = CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8))
    val configFile = new File(outputFolder, outName)

    Files.write(replacements.foldLeft(text)(
      (a, kv) => a.replace(kv._1, kv._2)
    ),
      configFile, Charsets.UTF_8)
    configFile.deleteOnExit()
    configFile
  }

  def addResourceLines(resource: String, buf: ArrayBuffer[String]): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    stream.withFinally(_.close()) { s =>
      buf ++= Source.fromInputStream(s).getLines()
    }
  }
}

trait DruidCluster {
  def zkConnectString : String
  def indexExists(dataSource: String): Boolean
  def overlordPort: Int
  def coordinatorPort: Int
  def brokerPort: Int
  def historicalPort: Int
}

object DruidCluster {
  lazy val instance : DruidCluster = if (false) {
    DruidTestCluster
  } else {
    LocalCluster
  }
}

object DruidTestCluster extends TestUtils with FreePortFinder with DruidCluster {

  val DRUID_RUNTIME_DIR = "druidRuntime"
  val DRUID_RUNTIME_LOCAL_STORAGE = s"$DRUID_RUNTIME_DIR/localStorage"
  val DRUID_RUNTIME_LOCAL_STORAGE_CACHE = s"$DRUID_RUNTIME_DIR/localStorageCache"
  val DRUID_RUNTIME_LOG = s"$DRUID_RUNTIME_DIR/logs"
  val DRUID_RUNTIME_INDEXING_LOG = s"$DRUID_RUNTIME_DIR/indexing-logs"
  val DRUID_RUNTIME_ENV_PACKAGE = "druidenv"
  val DAEMON_PROPERTIES_FILE_TEMPALTE = s"$DRUID_RUNTIME_ENV_PACKAGE/runtime.properties"
  val LOGGING_TEMPLATE = s"$DRUID_RUNTIME_ENV_PACKAGE/log4j2.xml"
  val JVM_CONFIG_TEMPLATE = s"$DRUID_RUNTIME_ENV_PACKAGE/jvm.config"

  val DAEMON_STARTUP_TIME = 2000L

  val daemonOutputthrdPool: ThreadPoolExecutor = {
    val prefix = "druidTestDaemonOut"
    val threadFactory =
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
    val threadPool = new ThreadPoolExecutor(
      4,
      4,
      10,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  private var zkServer: Option[TestingCluster] = None

  def zkConnectString = zkServer.get.getConnectString

  private val nameMap: MMap[String, Option[DruidTestDaemon]] = MMap(
    "coordinator" -> None,
    "broker" -> None,
    "overlord" -> None,
    "historical" -> None
  )

  def coordinator = nameMap("coordinator")

  def broker = nameMap("broker")

  def overlord = nameMap("overlord")

  def historical = nameMap("historical")

  def overlordPort: Int = servicePorts("overlord")

  def coordinatorPort: Int = servicePorts("coordinator")

  def brokerPort: Int = servicePorts("broker")

  def historicalPort: Int = servicePorts("historical")

  private lazy val stopSeq = Seq("historical", "overlord", "broker", "coordinator")
  private lazy val startSeq = Seq("coordinator", "overlord", "broker", "historical")

  startCluster

  private def startCluster: Unit = {

    zkServer = Some(
      retryOnErrors(ifException[BindException]) {
        new TestingCluster(1).withEffect(_.start())
      }
    )

    startSeq.foreach(startDaemon)

    Runtime.getRuntime.addShutdownHook {
      new Thread {
        override def run: Unit = {
          stopCluster
        }
      }
    }

    println(
      s"""
         |Druid Test Cluster Started:
         |  coordinator : $coordinatorPort
         |  broker      : $brokerPort
         |  overlord    : $overlordPort
         |  historical  : $historicalPort
       """.stripMargin)

    Thread.sleep(DAEMON_STARTUP_TIME)
  }

  override protected def finalize: Unit = {
    stopCluster
  }

  def stopCluster: Unit = {
    daemonOutputthrdPool.shutdownNow()
    stopSeq.foreach { d =>
      stopDaemon(nameMap(d))
      nameMap(d) = None
    }
    Try {
      zkServer.foreach(_.close())
    }
  }

  /**
    * inexact way to check if dataset is already indexed.
    *
    * @param dataSource
    * @return
    */
  def indexExists(dataSource: String): Boolean = {
    val f = new File(s"$DRUID_RUNTIME_LOCAL_STORAGE/$dataSource")
    f.exists()
  }

  private lazy val servicePorts: Map[String, Int] = {
    val ports = findFreePorts(5)
    nameMap.keySet.zip(ports.slice(0, 4)).toMap ++ Map("peon" -> ports.last)
  }

  private def stopDaemon(d: Option[DruidTestDaemon]): Unit = {
    d.foreach { d =>
      Try {
        d.process.destroyForcibly()
      }
      Try {
        d.logStream.close()
      }
    }
  }

  private def propertyMap(serviceName: String): Map[String, String] = {
    Map(
      ":DRUIDRUNTIME:" -> DRUID_RUNTIME_DIR,
      ":DRUIDLOCALSTORAGE:" -> DRUID_RUNTIME_LOCAL_STORAGE,
      ":DRUIDLOCALSTORAGECACHE:" -> DRUID_RUNTIME_LOCAL_STORAGE_CACHE,
      ":DRUIDLOGS:" -> DRUID_RUNTIME_LOG,
      ":DRUIDINDEXINGLOGS:" -> DRUID_RUNTIME_INDEXING_LOG,
      ":DRUIDPORT:" -> servicePorts(serviceName).toString,
      ":DRUIDSERVICE:" -> serviceName,
      ":ZKCONNECT:" -> zkConnectString,
      ":DRUIDFORKPORT:" -> servicePorts("peon").toString
    )
  }

  private def getClassPath(configFolder: File): String = {
    val propPath = configFolder.getPath
    val cp = classPathURLs().map(_.getPath).mkString(File.pathSeparator)
    Joiner.on(File.pathSeparator).join(propPath, cp)
  }

  private def startDaemon(serviceName: String): Unit = {

    val configFolder = tempFolder
    val pMap = propertyMap(serviceName)
    writeResource(DAEMON_PROPERTIES_FILE_TEMPALTE, pMap, configFolder)
    writeResource(LOGGING_TEMPLATE, pMap, configFolder)

    import scala.collection.JavaConversions._
    import scala.language.implicitConversions

    val cmdLine = ArrayBuffer[String]()
    cmdLine += "java"
    addResourceLines(JVM_CONFIG_TEMPLATE, cmdLine)
    cmdLine += s"-Ddruid.log.servicename=$serviceName"
    cmdLine += "-cp"
    cmdLine += getClassPath(configFolder)
    cmdLine += "io.druid.cli.Main"
    cmdLine += "server"
    cmdLine += serviceName

    val p = new ProcessBuilder(cmdLine).redirectErrorStream(true).start
    val daemonProcess = new DruidTestDaemon(serviceName, p)
    daemonOutputthrdPool.execute(new Runnable {
      def run: Unit = {
        ByteStreams.copy(daemonProcess.process.getInputStream, daemonProcess.logStream)
      }
    }
    )
    nameMap(serviceName) = Some(daemonProcess)

    Thread.sleep(DAEMON_STARTUP_TIME)

  }

}

object LocalCluster extends DruidCluster {
  def zkConnectString : String = "localhost"
  def indexExists(dataSource: String): Boolean = true
  def overlordPort: Int = 8090
  def coordinatorPort: Int = 8081
  def brokerPort: Int = 8082
  def historicalPort: Int = 8083
}
