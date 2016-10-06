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

package org.sparklinedata.druid

import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.base.Throwables
import org.apache.spark.Logging

import scala.reflect._

object RetryUtils extends Logging {

  var DEFAULT_RETRY_COUNT: Int = 10
  var DEFAULT_RETRY_SLEEP: Long = TimeUnit.SECONDS.toMillis(30)

  def retryUntil(callable: Callable[Boolean],
                 expectedValue: Boolean,
                 delayInMillis: Long,
                 retryCount: Int,
                 taskMessage: String) : Unit = {
    try {
      var currentTry: Int = 0
      while (callable.call != expectedValue) {
        if (currentTry > retryCount) {
          throw new IllegalStateException(
            s"Max number of retries[$retryCount] exceeded for Task[$taskMessage]. Failing."
          )
        }
        logInfo(s"Attempt[$currentTry]: " +
          s"Task $taskMessage still not complete. Next retry in $delayInMillis ms")
        Thread.sleep(delayInMillis)
        currentTry += 1
      }
    } catch {
      case e: Exception => {
        throw Throwables.propagate(e)
      }
    }
  }

  def ifException[E <: Exception : ClassTag] = (e: Exception) =>
    classTag[E].runtimeClass.isAssignableFrom(e.getClass)


  def backoff(start : Int, cap : Int) : Stream[Int] = {
    def next(current : Int) : Stream[Int] = {
      Stream.cons(current, next(Math.min(current *2, cap)))
    }
    next(start)
  }

  def execWithBackOff[X](taskMessage : String, f : Int => Option[X])(
    numTries : Int = Int.MaxValue, start : Int = 200, cap : Int = 5000) : X = {
    val b = backoff(start, cap).iterator
    var tries = 0
    while(tries < numTries) {
      val nextBackOff = b.next()
      f(nextBackOff) match {
        case Some(x) => return x
        case _ => {
          Thread.sleep(b.next)
          tries += 1
        }
      }
    }
    throw new IllegalStateException(
      s"Max number of retries[$numTries] exceeded for Task[$taskMessage]. Failing."
    )
  }

  def retryOnErrors[X](isTransients: (Exception => Boolean)*)(
    taskMessage: String,
    x: => X,
    numTries: Int = Int.MaxValue, start: Int = 200, cap: Int = 5000
  ): X = {
    execWithBackOff(taskMessage, { nextBackOff =>
      try Some(x) catch {
        case e: Exception if isTransients.find(_ (e)).isDefined =>
          logWarning(s"Transient error in $taskMessage, retrying after $nextBackOff ms")
          None
      }
    })(numTries, start, cap)

  }

  def retryOnError(isTransient: Exception => Boolean) = new {
    def apply[X](taskMessage: String,
                 x: => X)(
                 numTries: Int = Int.MaxValue, start: Int = 200, cap: Int = 5000) =
      retryOnErrors(isTransient)(taskMessage, x, numTries, start, cap)
  }
}
