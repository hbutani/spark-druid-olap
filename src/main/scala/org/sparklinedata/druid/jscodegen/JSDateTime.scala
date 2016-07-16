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

package org.sparklinedata.druid.jscodegen


private[jscodegen] case class JSDateTimeCtx(val tz_id: String, val ctx: JSCodeGenerator) {
  private[jscodegen] val tzVar: String = ctx.makeUniqueVarName
  private[jscodegen] val isoFormatterVar: String = ctx.makeUniqueVarName
  private[jscodegen] val isoFormatterWithTZVar: String = ctx.makeUniqueVarName

  private[jscodegen] var createJodaTZ: Boolean = false
  private[jscodegen] var createJodaISOFormatter: Boolean = false
  private[jscodegen] var createJodaISOFormatterWithTZ: Boolean = false

  private[jscodegen] def dateTimeInitCode: String = {
    var dtInitCode = ""
    if (createJodaTZ) {
      dtInitCode =
        s"""var ${tzVar} = new org.joda.time.DateTimeZone.forID("${tz_id}");""".stripMargin
    }
    if (createJodaISOFormatterWithTZ || createJodaISOFormatter) {
      dtInitCode += s"var ${isoFormatterVar} = " +
        s"org.joda.time.format.ISODateTimeFormat.dateTimeParser();"
    }
    if (createJodaISOFormatterWithTZ) {
      dtInitCode += s"var ${isoFormatterWithTZVar} = " +
        s"${isoFormatterVar}.withZone(${tzVar});"
    }

    dtInitCode
  }
}

private[jscodegen] object JSDateTimeCtx {
  private val dateFormat = "yyyy-MM-dd"
  private val timeStampFormat = "yyyy-MM-dd HH:mm:ss"
  private val mSecsInDay = 86400000

  private[jscodegen] def dtInFormatCode(f: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTZ = true
    s"""org.joda.time.format.forPattern($f).withZone(${ctx.tzVar})""".stripMargin
  }

  private[jscodegen] def dateToStrCode(jdt: String) =
    s"""($jdt.toString("$dateFormat"))""".stripMargin

  private[jscodegen] def stringToDateCode(ts: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaISOFormatter = true
    s"org.joda.time.LocalDate.parse(${ts}, ${ctx.isoFormatterVar})"
  }

  private[jscodegen] def longToDateCode(ts: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTZ = true
    s"new org.joda.time.LocalDate(${ts}, ${ctx.tzVar})"
  }

  private[jscodegen] def noDaysToDateCode(ts: String) =
    s"new org.joda.time.LocalDate(${ts} * $mSecsInDay)"

  private[jscodegen] def trunc(ld: String, format: String): Option[String] =
    format.toUpperCase match {
      case "YEAR" | "YYYY" | "YY" => Some(s"$ld.withDayOfMonth(1).withMonthOfYear(1)")
      case "MON" | "MONTH" | "MM" => Some(s"$ld.withDayOfMonth(1)")
      case _ => None
    }

  private[jscodegen] def dtToDateCode(ts: String) = s"${ts}.toLocalDate()"

  private[jscodegen] def dtToStrCode(jdt: String,
                                     f: String = s""""${timeStampFormat}"""".stripMargin) =
    s"""($jdt.toString(${f}))""".stripMargin

  private[jscodegen] def stringToDTCode(sd: String, f: String) =
    s"""org.joda.time.DateTime.parse($sd, $f)""".stripMargin

  private[jscodegen] def stringToISODTCode(sd: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTZ = true
    ctx.createJodaISOFormatterWithTZ = true
    stringToDTCode(s"""${sd}.replace(" ", "T")""".stripMargin, ctx.isoFormatterWithTZVar)
  }

  private[jscodegen] def longToISODTCode(l: Any, ctx: JSDateTimeCtx): String = {
    ctx.createJodaTZ = true
    s"(new org.joda.time.DateTime($l, ${ctx.tzVar}))"
  }

  private[jscodegen] def localDateToDTCode(ld: String, ctx: JSDateTimeCtx): String = {
    ctx.createJodaTZ = true
    s"$ld.toDateTimeAtStartOfDay(${ctx.tzVar})"
  }

  private[jscodegen] def dtToLongCode(ts: String) = s"$ts.getMillis()*1000"

  private[jscodegen] def dtToSecondsCode(ts: String) = s"Math.floor($ts.getMillis()/1000)"


  private[jscodegen] def dateAdd(dt: String, nd: String) = s"$dt.plusDays($nd)"

  private[jscodegen] def dateSub(dt: String, nd: String) = s"$dt.minusDays($nd)"

  private[jscodegen] def dateDiff(ed: String, sd: String) =
    s"org.joda.time.Days.daysBetween($sd, $ed).getDays()"

  private[jscodegen] def dTYear(dt: String) = s"$dt.getYear()"

  private[jscodegen] def dTQuarter(dt: String) = s"(Math.floor(($dt.getMonthOfYear() - 1) / 3) + 1)"

  private[jscodegen] def dTMonth(dt: String) = s"$dt.getMonthOfYear()"

  private[jscodegen] def dTDayOfMonth(dt: String) = s"$dt.getDayOfMonth()"

  private[jscodegen] def dTWeekOfYear(dt: String) = s"$dt.getWeekOfWeekyear()"

  private[jscodegen] def dTHour(dt: String) = s"$dt.getHourOfDay()"

  private[jscodegen] def dTMinute(dt: String) = s"$dt.getMinuteOfHour()"

  private[jscodegen] def dTSecond(ts: String) = s"$ts.getSecondOfMinute()"

  private[jscodegen] def dComparison(l: String, r: String, op: String): Option[String] = op match {
    case " < " => Some(s"""(($l).isBefore($r))""".stripMargin)
    case " <= " => Some(s"""(($l).compareTo($r) <= 0)""".stripMargin)
    case " == " => Some(s"""(($l).equals($r))""".stripMargin)
    case " >= " => Some(s"""(($l).compareTo($r) >= 0)""".stripMargin)
    case " > " => Some(s"""(($l).isAfter($r) > 0)""".stripMargin)
    case _ => None
  }
}
