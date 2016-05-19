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
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths
import org.apache.spark.util.SparklineThreadUtils
import org.sparklinedata.druid.{DruidDataSourceException, Utils}
import org.sparklinedata.druid.metadata._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.Try

class CuratorConnection(val zkHosts : String,
                        val options : DruidRelationOptions,
                        val cache : DruidMetadataCache,
                        execSvc : ExecutorService) {

  val discoveryPath = ZKPaths.makePath(options.zkDruidPath, "discovery")
  val announcementPath = ZKPaths.makePath(options.zkDruidPath, "announcements")

  val framework: CuratorFramework = CuratorFrameworkFactory.builder.connectString(
    zkHosts).
    sessionTimeoutMs(options.zkSessionTimeoutMs).
    retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30)).
    compressionProvider(
      new PotentiallyGzippedCompressionProvider(options.zkEnableCompression)
    ).build

  val childrenCache : PathChildrenCache = new PathChildrenCache(
    framework,
    announcementPath,
    true,
    true,
    execSvc
  )

  val listener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED |
             PathChildrenCacheEvent.Type.CHILD_REMOVED |
             PathChildrenCacheEvent.Type.CHILD_UPDATED => cache.clearCache(zkHosts)
        case _ => ()
      }
    }
  }

  childrenCache.getListenable.addListener(listener)

  framework.start()
  childrenCache.start(StartMode.BUILD_INITIAL_CACHE)

  SparklineThreadUtils.addShutdownHook {() =>
    Try {childrenCache.close()}
    Try {framework.close()}
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
