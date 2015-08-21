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

import com.github.nscala_time.time.Imports._
import org.sparklinedata.druid.metadata.DruidRelationInfo



case class QueryIntervals(drInfo : DruidRelationInfo,
                      intervals : List[Interval] = List()) {

  val indexIntervals = drInfo.druidDS.intervals

  def get : List[Interval] = if (intervals.isEmpty) indexIntervals else intervals

  private def indexInterval(dT : DateTime) : Option[Interval] = {
    indexIntervals.find(i => i.contains(dT))
  }

  private final def maxDate(d1 : DateTime, d2 : DateTime) : DateTime = {
    if ( d1 >= d2) d1 else d2
  }

  private def combine(i1 : Interval, i2 : Interval) : Interval = (i1, i2) match {
    case (i1, i2) if i1.start <= i2.start => new Interval(i1.start, maxDate(i1.end,i2.end))
    case _ => new Interval(i2.start, maxDate(i1.end,i2.end))
  }

  /**
   * - if this is the first queryInterval, add it.
   * - if the new Interval overlaps with the current QueryInterval set interval to the overlap.
   * - otherwise, the interval is an empty Interval.
   * @param i
   * @return
   */
  private def add(i : Interval) : QueryIntervals = {

    if ( intervals.isEmpty) {
      QueryIntervals(drInfo, List(i))
    } else {
      val qI = intervals.head
      if ( qI.overlaps(i)) {
        QueryIntervals(drInfo, List(qI.overlap(i)))
      } else {
        val iI = indexIntervals.head
        QueryIntervals(drInfo, List(iI.withEnd(iI.start)))
      }
    }
  }

  def gtCond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withStart(dT + 1.millis)
      add(cI)
    }
  }

  def gtECond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withStart(dT)
      add(cI)
    }
  }

  def ltCond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withEnd(dT)
      add(cI)
    }
  }

  def ltECond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withEnd(dT + 1.millis)
      add(cI)
    }
  }

}
