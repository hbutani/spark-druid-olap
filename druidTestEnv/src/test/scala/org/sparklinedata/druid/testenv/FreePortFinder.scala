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

import java.net.ServerSocket
import scala.collection.mutable.ArrayBuffer


trait FreePortFinder {

  val MIN_PORT_NUMBER = 8000
  val MAX_PORT_NUMBER = 49151

  def findFreePorts(numPorts: Int = 1): List[Int] = {
    val freePorts = ArrayBuffer[Int]()
    (MIN_PORT_NUMBER until MAX_PORT_NUMBER).foreach { i =>
      if (available(i)) {
        freePorts += i
      }
      if (freePorts.size == numPorts) {
        return freePorts.toList
      }
    }
    throw new RuntimeException(s"Could not find $numPorts ports between " +
      MIN_PORT_NUMBER + " and " + MAX_PORT_NUMBER)
  }

  def available(port: Int): Boolean = {
    var serverSocket: Option[ServerSocket] = None
    try {
      serverSocket = Some(new ServerSocket(port))
      serverSocket.get.setReuseAddress(true);
      return true
    } catch {
      case _: Throwable => return false
    } finally {
      serverSocket.map(_.close())
    }
  }
}
