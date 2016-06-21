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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{LongType, TimestampType, DateType, StringType}
import org.joda.time.DateTime
import org.sparklinedata.druid.metadata.{DruidColumn, DruidDataSource}
import org.sparklinedata.spark.dateTime.Functions._

object PeriodExtractor {
  def unapply(e : Expression) : Option[Period] = e match {
    case ScalaUDF(fn, _, Seq(Literal(value, StringType)), _) if fn == periodFn => {
      Some(periodFn(value.toString))
    }
    case _ => None
  }
}

object DateTimeExtractor {
  def unapply(e : Expression) : Option[DateTime] = e match {
    case ScalaUDF(fn, _, Seq(Literal(value, StringType)), _)
      if fn == dateTimeFn => Some(dateTimeFn(value.toString))
    case ScalaUDF(fn, _, Seq(Literal(value, StringType)), _)
      if fn == dateTimeWithTZFn => Some(dateTimeWithTZFn(value.toString))
    case ScalaUDF(fn, _, Seq(Literal(value, StringType), Literal(pattern, StringType)), _)
      if fn == dateTimeWithFormatFn =>
      Some(dateTimeWithFormatFn(value.toString, pattern.toString))
    case ScalaUDF(fn, _, Seq(Literal(value, StringType), Literal(pattern, StringType)), _)
      if fn == dateTimeWithFormatAndTZFn =>
      Some(dateTimeWithFormatFn(value.toString, pattern.toString))
    case ScalaUDF(fn, _, Seq(DateTimeExtractor(dT), PeriodExtractor(p)), _)
      if fn == datePlusFn => Some(datePlusFn(dT, p))
    case ScalaUDF(fn, _, Seq(DateTimeExtractor(dT), PeriodExtractor(p)), _)
      if fn == dateMinusFn => Some(dateMinusFn(dT, p))
      // Todo add with functions here.
    case _ => None
  }

}

object IntervalConditionType extends Enumeration {
  val GT = Value
  val GTE = Value
  val LT = Value
  val LTE = Value
}

case class IntervalCondition(typ : IntervalConditionType.Value, dt : DateTime)

class TimeReferenceExtractor(val dqb : DruidQueryBuilder, indexColumn : Boolean = true) {

  val drInfo = dqb.drInfo

  def unapply(e : Expression) : Option[String] = e match {
    case ScalaUDF(fn, _, Seq(AttributeReference(nm, _, _, _)), _)
      if fn == dateTimeFn  => {
      val dC = dqb.druidColumn(nm)
      // scalastyle:off if.brace
      if ( dC.isDefined && (!indexColumn || dC.get.name == DruidDataSource.TIME_COLUMN_NAME) )
        Some(nm)
      else None
    }
    case _ => None
  }
}

class IntervalConditionExtractor(val dqb : DruidQueryBuilder) {

  val timeRefExtractor = new TimeReferenceExtractor(dqb, true)

  def unapply(e : Expression) : Option[IntervalCondition] = e match {
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsBeforeFn => Some(IntervalCondition(IntervalConditionType.LT, dT))
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)),_)
      if fn == dateIsBeforeOrEqualFn => Some(IntervalCondition(IntervalConditionType.LTE, dT))
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsAfterFn => Some(IntervalCondition(IntervalConditionType.GT, dT))
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsAfterOrEqualFn => Some(IntervalCondition(IntervalConditionType.GTE, dT))
    case _ => None
  }
}

/**
 * Return (DruidColumn, ComparisonOperator, DateTimeValue)
  *
  * @param dqb
 */
class DateTimeConditionExtractor(val dqb : DruidQueryBuilder) {

  val timeRefExtractor = new TimeReferenceExtractor(dqb, false)

  def unapply(e : Expression) : Option[(String, String, String)] = e match {
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsBeforeFn => Some((timDm, "<", dT.toString()))
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsBeforeOrEqualFn => Some((timDm, "<=", dT.toString()))
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsAfterFn => Some((timDm, ">", dT.toString()))
    case ScalaUDF(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)), _)
      if fn == dateIsAfterOrEqualFn => Some((timDm, ">=", dT.toString()))
    case _ => None
  }
}

/**
 * Extract expressions of the form:
 * - dateTime(dimCol)
 * - withZone(dateTime(dimCol))
  *
  * @param dqb
 */
class DateTimeWithZoneExtractor(val dqb : DruidQueryBuilder) {

  val rExtractor = this

  def unapply(e : Expression) : Option[(String, DruidColumn, Option[String])] = e match {
    case ScalaUDF(fn, _, Seq(AttributeReference(nm, _, _, _)), _)
      if fn == dateTimeFn  => {
      val dC = dqb.druidColumn(nm)
      dC.filter(_.isDimension()).map((nm, _, Some(Utils.defaultTZ)))
    }
    case ScalaUDF(fn, _, Seq(rExtractor((nm, dC, None)), Literal(tzId, StringType)), _)
      if fn == withZoneFn  => {
      Some(nm, dC, Some(tzId.toString))
    }
    case _ => None
  }
}

/**
 * Extract expressions that extract a dateTime element of a dimColumn that is parsed as Date
 * For e.g.:
 * - year(dateTime(dimCol))
 * - monthOfYearName(withZone(dateTime(dimCol)))
 *
 * @param dqb
 */
class TimeElementExtractor(val dqb : DruidQueryBuilder) {

  val functionToFormatMap : Map[AnyRef, String] = Map(
    eraFn -> "GG",
    centuryOfEraFn -> "CC",
    yearOfEraFn -> "YYYY",
    yearOfCenturyFn -> "yy",
    yearFn -> "yyyy",
    weekyearFn -> "xxxx",
    monthOfYearFn -> "MM",
    monthOfYearNameFn -> "MMMM",
    weekOfWeekyearFn -> "ww",
    dayOfYearFn -> "DDD",
    dayOfMonthFn -> "dd",
    dayOfWeekFn -> "ee",
    dayOfWeekNameFn -> "EEEE",
    hourOfDayFn -> "HH",
    secondOfMinuteFn -> "ss"

  )
  val dateTimeWithZoneExtractor = new DateTimeWithZoneExtractor(dqb)

  def unapply(e : Expression) : Option[(String, DruidColumn, Option[String], String)] = e match {
    case ScalaUDF(fn, _, Seq(dateTimeWithZoneExtractor((nm, dC, tz))), _)
      if functionToFormatMap.contains(fn)  => {
      val fmt = functionToFormatMap(fn)
      Some(nm, dC, tz, fmt)
    }
    case _ => None
  }

}

/**
  *
  * @param outputName the name of the column in the Druid resultset
  * @param druidColumn the Druid Dimension this is mapped to
  * @param formatToApply in Druid Spark DateTime Expressions handled
  *                      as [[TimeFormatExtractionFunctionSpec]]; this specifies
  *                      the format to apply on the Druid Dimension.
  * @param tzForFormat see above
  * @param pushedExpression this controls the expression evlaution that happens
  *                         on return from Druid. So for expression like
  *                         {{{to_date(cast(dateCol as DateType))}}},
  *                         {{{to_date(cast(DruidValue, DatetYpe))}}} is evaluated
  *                         on the resultset of Druid. This is required because
  *                         Dates are Ints and Timestamps are Longs in Spark, whereas
  *                         the value coming out of Druid is an ISO DateTime String.
  * @param inputFormat  format to use to parse input value.
  */
case class DateTimeGroupingElem(
                               outputName : String,
                               druidColumn : DruidColumn,
                               formatToApply : String,
                               tzForFormat : Option[String],
                               pushedExpression : Expression,
                               inputFormat : Option[String] = None
                               )

class DruidColumnExtractor(val dqb : DruidQueryBuilder) {
  def unapply(e : Expression) : Option[DruidColumn] = e match {
    case AttributeReference(nm, _, _, _) => {
      val dC = dqb.druidColumn(nm)
      dC.filter(_.isDimension())
    }
    case _ => None
  }
}

class SparkNativeTimeElementExtractor(val dqb : DruidQueryBuilder) {

  import SparkNativeTimeElementExtractor._

  val dcExtractor = new DruidColumnExtractor(dqb)
  val timeExtractor = this


  def unapply(e : Expression) : Option[DateTimeGroupingElem] = e match {
    case dcExtractor(dc) if e.dataType == DateType =>
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, DATE_FORMAT, Some(Utils.defaultTZ), e))
    case Cast(c@dcExtractor(dc), DateType) =>
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, DATE_FORMAT, Some(Utils.defaultTZ), c))
    case Cast(timeExtractor(dtGrp), DateType) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, DATE_FORMAT,
        dtGrp.tzForFormat, dtGrp.pushedExpression))
    case dcExtractor(dc) if e.dataType == TimestampType =>
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, TIMESTAMP_FORMAT, Some(Utils.defaultTZ), e))
    case Cast(c@dcExtractor(dc), TimestampType) =>
      // TODO: handle a tsFmt coming from below, see test fromUnixTimestamp
      Some(DateTimeGroupingElem(dqb.nextAlias, dc, TIMESTAMP_FORMAT, Some(Utils.defaultTZ), c))
    case Cast(timeExtractor(dtGrp), TimestampType) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, TIMESTAMP_FORMAT,
        dtGrp.tzForFormat, dtGrp.pushedExpression))
    case ToDate(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, DATE_FORMAT,
        dtGrp.tzForFormat, dtGrp.pushedExpression)
      )
    case e@Year(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, YEAR_FORMAT,
        dtGrp.tzForFormat, e))
    case e@DayOfMonth(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, DAY_OF_MONTH_FORMAT,
        dtGrp.tzForFormat, e))
    case e@DayOfYear(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, DAY_OF_YEAR_FORMAT,
        dtGrp.tzForFormat, e))
    case e@Month(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, MONTH_FORMAT,
        dtGrp.tzForFormat, e))
    case e@WeekOfYear(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, WEEKOFYEAR_FORMAT,
        dtGrp.tzForFormat, e))
    case e@Hour(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, HOUR_FORMAT,
        dtGrp.tzForFormat, e))
    case e@Minute(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, MINUTE_FORMAT,
        dtGrp.tzForFormat, e))
    case e@Second(timeExtractor(dtGrp)) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, SECOND_FORMAT,
        dtGrp.tzForFormat, e))
    case UnixTimestamp(timeExtractor(dtGrp), Literal(iFmt, StringType)) =>
      Some(
        DateTimeGroupingElem(dtGrp.outputName,
          dtGrp.druidColumn, TIMESTAMP_FORMAT,
          dtGrp.tzForFormat, dtGrp.pushedExpression,
          Some(iFmt.toString))
      )
    case UnixTimestamp(c@dcExtractor(dc), Literal(iFmt, StringType)) =>
      Some(
        DateTimeGroupingElem(dqb.nextAlias,
          dc, TIMESTAMP_FORMAT,
          None, c,
          Some(iFmt.toString))
      )
    case FromUnixTime(timeExtractor(dtGrp), Literal(oFmt, StringType)) =>
      Some(
        DateTimeGroupingElem(dtGrp.outputName,
          dtGrp.druidColumn, oFmt.toString,
          dtGrp.tzForFormat, e)
      )
    case FromUnixTime(c@dcExtractor(dc), Literal(oFmt, StringType)) =>
      Some(
        DateTimeGroupingElem(dqb.nextAlias,
          dc, oFmt.toString,
          None, e)
      )

    case FromUTCTimestamp(timeExtractor(dtGrp), Literal(oTZ, StringType)) =>
      Some(
        DateTimeGroupingElem(dtGrp.outputName,
          dtGrp.druidColumn, TIMESTAMP_FORMAT,
          Some(oTZ.toString), dtGrp.pushedExpression)
      )
    case FromUTCTimestamp(c@dcExtractor(dc), Literal(oTZ, StringType)) =>
      Some(
        DateTimeGroupingElem(dqb.nextAlias,
          dc, TIMESTAMP_FORMAT,
          Some(oTZ.toString), e)
      )
    case ToUTCTimestamp(timeExtractor(dtGrp), _) =>
      Some(
        DateTimeGroupingElem(dtGrp.outputName,
          dtGrp.druidColumn, TIMESTAMP_FORMAT,
          None, dtGrp.pushedExpression)
      )
    case ToUTCTimestamp(c@dcExtractor(dc), _) =>
      Some(
        DateTimeGroupingElem(dqb.nextAlias,
          dc, TIMESTAMP_FORMAT,
          None, e)
      )
    case e@Cast(timeExtractor(dtGrp), StringType) =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, dtGrp.formatToApply,
        dtGrp.tzForFormat, e))
      // tableau timestamp
    case e@Cast(Concat(Seq(timeExtractor(dtGrp), Literal(v, StringType))),
    TimestampType)
      if dtGrp.formatToApply == DATE_FORMAT && " 00:00:00" == v.toString =>
      Some(DateTimeGroupingElem(dtGrp.outputName,
        dtGrp.druidColumn, TIMESTAMP_DATEZERO_FORMAT,
        dtGrp.tzForFormat, e))
    case _ => None
  }
}

object SparkNativeTimeElementExtractor {
  val DATE_FORMAT = "YYYY-MM-dd"
  val TIMESTAMP_FORMAT = "YYYY-MM-dd HH:mm:ss"
  val TIMESTAMP_DATEZERO_FORMAT = "YYYY-MM-dd 00:00:00"

  val YEAR_FORMAT = "YYYY"
  val MONTH_FORMAT = "MM"
  val WEEKOFYEAR_FORMAT = "ww"
  val DAY_OF_MONTH_FORMAT = "dd"
  val DAY_OF_YEAR_FORMAT = "DD"

  val HOUR_FORMAT = "HH"
  val MINUTE_FORMAT = "mm"
  val SECOND_FORMAT = "ss"
}

class SparkIntervalConditionExtractor(val dqb : DruidQueryBuilder) {

  import SparkNativeTimeElementExtractor._

  val timeRefExtractor = new SparkNativeTimeElementExtractor(dqb)

  private def millisSinceEpoch(value : Any) : DateTime =
    new DateTime(value.toString.toLong / 1000)

  def unapply(e : Expression) : Option[IntervalCondition] = e match {
    case LessThan(timeRefExtractor(dtGrp), Literal(value, TimestampType))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.LT, millisSinceEpoch(value)))
    case LessThan(Literal(value, TimestampType), timeRefExtractor(dtGrp))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.GT, millisSinceEpoch(value)))
    case LessThanOrEqual(timeRefExtractor(dtGrp), Literal(value, TimestampType))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.LTE, millisSinceEpoch(value)))
    case LessThanOrEqual(Literal(value, TimestampType), timeRefExtractor(dtGrp))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.GTE, millisSinceEpoch(value)))
    case GreaterThan(timeRefExtractor(dtGrp), Literal(value, TimestampType))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.GT, millisSinceEpoch(value)))
    case GreaterThan(Literal(value, TimestampType), timeRefExtractor(dtGrp))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.LT, millisSinceEpoch(value)))
    case GreaterThanOrEqual(timeRefExtractor(dtGrp), Literal(value, TimestampType))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.GTE, millisSinceEpoch(value)))
    case GreaterThanOrEqual(Literal(value, TimestampType), timeRefExtractor(dtGrp))
      if dtGrp.druidColumn.name == DruidDataSource.TIME_COLUMN_NAME &&
        (dtGrp.formatToApply == TIMESTAMP_FORMAT ||
          dtGrp.formatToApply == TIMESTAMP_DATEZERO_FORMAT) =>
      Some(IntervalCondition(IntervalConditionType.LTE, millisSinceEpoch(value)))
    case _ => None
  }
}
