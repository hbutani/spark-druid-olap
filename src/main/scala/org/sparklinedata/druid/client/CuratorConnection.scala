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

import java.io.IOException
import java.util.concurrent.ExecutorService

import org.apache.curator.framework.api.CompressionProvider
import org.apache.curator.framework.imps.GzipCompressionProvider
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.utils.{CloseableUtils, ZKPaths}
import org.apache.spark.Logging
import org.apache.spark.util.SparklineThreadUtils
import org.sparklinedata.druid.{DruidDataSourceException, Utils}
import org.sparklinedata.druid.metadata._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.{Map => MMap}
import scala.util.Try

class CuratorConnection(val zkHosts : String,
                        val options : DruidRelationOptions,
                        val cache : DruidMetadataCache,
                        execSvc : ExecutorService) extends Logging {

  val serverSegmentsCache : MMap[String, PathChildrenCache] = MMap()
  val serverSegmentCacheLock = new Object

  val discoveryPath = ZKPaths.makePath(options.zkDruidPath, "discovery")
  val announcementPath = ZKPaths.makePath(options.zkDruidPath, "announcements")
  val serverSegmentsPath = ZKPaths.makePath(options.zkDruidPath, "segments")

  val framework: CuratorFramework = CuratorFrameworkFactory.builder.connectString(
    zkHosts).
    sessionTimeoutMs(options.zkSessionTimeoutMs).
    retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30)).
    compressionProvider(
      new PotentiallyGzippedCompressionProvider(options.zkEnableCompression)
    ).build

  val announcementsCache : PathChildrenCache = new PathChildrenCache(
    framework,
    announcementPath,
    true,
    true,
    execSvc
  )

  val serverSegmentsPathCache : PathChildrenCache = new PathChildrenCache(
    framework,
    serverSegmentsPath,
    true,
    true,
    execSvc
  )

  val listener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED |
             PathChildrenCacheEvent.Type.CHILD_REMOVED  => {
          println(s"Event received: ${event.getType}")
          cache.clearCache(zkHosts)
        }
        case _ => ()
      }
    }
  }

  val serverSegmentsListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED => {
          serverSegmentCacheLock.synchronized {
            val child: ChildData = event.getData
            val key = getServerKey(event)
            if (serverSegmentsCache.contains(key)) {
              log.error(
                "New node[%s] but there was already one.  That's not good, ignoring new one.",
                child.getPath
              )
            } else {
              val segmentsPath: String = String.format("%s/%s", serverSegmentsPath, key)
              val segmentsCache: PathChildrenCache = new PathChildrenCache(
                framework,
                segmentsPath,
                true,
                true,
                execSvc
              )
              segmentsCache.getListenable.addListener(listener)
              serverSegmentsCache(key) = segmentsCache
              log.debug("Starting inventory cache for %s, inventoryPath %s", key, segmentsPath)
              segmentsCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)
            }
          }
        }
        case PathChildrenCacheEvent.Type.CHILD_REMOVED => {
          serverSegmentCacheLock.synchronized {
            val child: ChildData = event.getData
            val key = getServerKey(event)
            val segmentsCache: Option[PathChildrenCache] = serverSegmentsCache.remove(key)
            if (segmentsCache.isDefined) {
              log.debug("Closing inventory cache for %s. Also removing listeners.", key)
              segmentsCache.get.getListenable.clear()
              segmentsCache.get.close()
            } else log.warn("Container[%s] removed that wasn't a container!?", child.getPath)
          }
        }
        case _ => ()
      }
    }
  }

  announcementsCache.getListenable.addListener(listener)
  serverSegmentsPathCache.getListenable.addListener(serverSegmentsListener)

  framework.start()
  announcementsCache.start(StartMode.BUILD_INITIAL_CACHE)
  serverSegmentsPathCache.start(StartMode.POST_INITIALIZED_EVENT)

  // trigger loading class CloseableUtils
  CloseableUtils.closeQuietly(null)

  SparklineThreadUtils.addShutdownHook {() =>
    Try {announcementsCache.close()}
    Try {serverSegmentsPathCache.close()}
    Try {
      serverSegmentCacheLock.synchronized {
        serverSegmentsCache.values.foreach { inventoryCache =>
          inventoryCache.getListenable.clear()
          inventoryCache.close()
        }
      }
    }
    Try {CloseableUtils.closeQuietly(framework)}
  }

  /*
  Not using Curator Discovery extension because of mvn issues.
  Code using discovery extension:

  import org.apache.curator.x.discovery.details.{JsonInstanceSerializer, ServiceDiscoveryImpl}

  val discovery = new ServiceDiscoveryImpl(
  framework,
  s"${options.zkDruidPath}/discovery",
  new JsonInstanceSerializer(classOf[DruidNode]),
  null)

  def getService(name : String) : String = {
  import collection.JavaConversions._
  discovery.queryForInstances(name).map(
    s => s"${s.getAddress}:${s.getPort}"
  ).headOption.getOrElse(
    throw new DruidDataSourceException(s"Failed to get '$name' for '$zkHosts'")
  )
}
 */

  def getService(name : String) : String = {

    import Utils._
    import collection.JavaConversions._

    val n = if (options.zkQualifyDiscoveryNames ) s"${options.zkDruidPath}:$name" else name

    val sPath = ZKPaths.makePath(discoveryPath, n)
    val b: java.util.List[String] =
      framework.getChildren().forPath(sPath)

    try {
      val idPath = ZKPaths.makePath(sPath, b.head)
      val bytes: Array[Byte] = framework.getData.forPath(idPath)
      val nd = parse(new String(bytes)).extract[DruidNode]
      s"${nd.address}:${nd.port}"
    } catch {
      case e : Exception =>
        throw new DruidDataSourceException(s"Failed to get '$name' for '$zkHosts'", e)
    }
  }

  def getBroker : String = {
    getService("broker")
  }

  def getCoordinator : String = {
    getService("coordinator")
  }

  private def getServerKey(event: PathChildrenCacheEvent) : String = {
    val child: ChildData = event.getData

    val data: Array[Byte] = getZkDataForNode(child.getPath)
    if (data == null) {
      log.info("Ignoring event: Type - %s , Path - %s , Version - %s",
        Array(event.getType, child.getPath, child.getStat.getVersion))
      null
    } else {
      ZKPaths.getNodeFromPath(child.getPath)
    }
  }

  private def getZkDataForNode(path: String): Array[Byte] = {
    try {
      framework.getData.decompressed.forPath(path)
    } catch {
      case ex: Exception => {
        log.warn(s"Exception while getting data for node $path", ex)
        null
      }
    }
  }

}

/*
 * copied from druid code base.
 */
class PotentiallyGzippedCompressionProvider(val compressOutput: Boolean)
  extends CompressionProvider {

  private val base: GzipCompressionProvider = new GzipCompressionProvider


  @throws[Exception]
  def compress(path: String, data: Array[Byte]): Array[Byte] = {
    return if (compressOutput) base.compress(path, data)
    else data
  }

  @throws[Exception]
  def decompress(path: String, data: Array[Byte]): Array[Byte] = {
    try {
      return base.decompress(path, data)
    }
    catch {
      case e: IOException => {
        return data
      }
    }
  }
}
