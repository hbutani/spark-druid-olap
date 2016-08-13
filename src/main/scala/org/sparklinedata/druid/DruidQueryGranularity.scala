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

import jodd.datetime.Period
import org.joda.time.chrono.ISOChronology
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.json4s.MappingException

import scala.util.Try
import scala.language.postfixOps

sealed trait DruidQueryGranularity extends Serializable {

  def ndv(ins : List[Interval]) : Long

}

object DruidQueryGranularity {

  import Utils._
  import org.json4s.jackson.sparklinedata.SmileJsonMethods._


  @throws[MappingException]
  def apply(s : String) : DruidQueryGranularity = s match {
    case n if n.toUpperCase().equals("NONE") => NoneGranularity
    case a if a.toUpperCase().equals("ALL") => AllGranularity
    case s if s.toUpperCase().equals("SECOND") => DurationGranularity(1000L)
    case m if m.toLowerCase().equals("MINUTE") => DurationGranularity(60 * 1000L)
    case fm if fm.toLowerCase().equals("FIFTEEN_MINUTE") => DurationGranularity(15 * 60 * 1000L)
    case tm if tm.toLowerCase().equals("THIRTY_MINUTE") => DurationGranularity(15 * 60 * 1000L)
    case h if h.toLowerCase().equals("HOUR") => DurationGranularity(3600 * 1000L)
    case d if d.toLowerCase().equals("DAY") => DurationGranularity(24 * 3600 * 1000L)
    case _ => {
      val jv = parse(s)
      Try {
        jv.extract[PeriodGranularity]
      } recover {
        case _ => jv.extract[DurationGranularity]
      } get
    }
  }
}

object AllGranularity extends DruidQueryGranularity {
  def ndv(ins : List[Interval]) = 1L
}

object NoneGranularity extends DruidQueryGranularity {
  def ndv(ins : List[Interval]) = Utils.intervalsMillis(ins)
}


case class PeriodGranularity(
                              period: Period,
                              origin: DateTime,
                              tz: DateTimeZone) extends DruidQueryGranularity {

  val chronology = if (tz == null) ISOChronology.getInstanceUTC else ISOChronology.getInstance(tz)
  lazy val origMillis = if (origin == null) {
    new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(chronology.getZone).getMillis
  }
  else {
    origin.getMillis
  }
  lazy val periodMillis = period.getMilliseconds

  def ndv(ins : List[Interval]) = {
    val boundedIns = ins.map { i =>
      i.withStartMillis(Math.max(origMillis, i.getStartMillis)).
        withEndMillis(Math.min(origMillis, i.getEndMillis))
    }
    Utils.intervalsMillis(boundedIns) / periodMillis
  }
}

case class DurationGranularity(
                                duration: Long,
                              origin: DateTime = null) extends DruidQueryGranularity {

  lazy val origMillis = if (origin == null) 0 else origin.getMillis

  def ndv(ins : List[Interval]) = {
    val boundedIns = ins.map { i =>
      i.withStartMillis(Math.max(origMillis, i.getStartMillis)).
        withEndMillis(Math.min(origMillis, i.getEndMillis))
    }
    Utils.intervalsMillis(boundedIns) / duration
  }
}


