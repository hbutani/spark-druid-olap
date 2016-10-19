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

package org.apache.spark.sql

import org.apache.spark.internal.Logging


trait SPLLogging extends Logging {

  override def logInfo(msg: => String) {
    super.logInfo(msg)
  }

  override def logDebug(msg: => String) {
    super.logDebug(msg)
  }

  override def logTrace(msg: => String) {
    super.logTrace(msg)
  }

  override def logWarning(msg: => String) {
    super.logWarning(msg)
  }

  override def logError(msg: => String) {
    super.logError(msg)
  }

  override def logInfo(msg: => String, throwable: Throwable) {
    super.logInfo(msg, throwable)
  }

  override def logDebug(msg: => String, throwable: Throwable) {
    super.logDebug(msg, throwable)
  }

  override def logTrace(msg: => String, throwable: Throwable) {
    super.logTrace(msg, throwable)
  }

  override def logWarning(msg: => String, throwable: Throwable) {
    super.logWarning(msg, throwable)
  }

  override def logError(msg: => String, throwable: Throwable) {
    super.logError(msg, throwable)
  }

}
