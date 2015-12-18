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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal, ScalaUDF}
import org.apache.spark.sql.types.StringType
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
 * @param dqb
 */
class DateTimeWithZoneExtractor(val dqb : DruidQueryBuilder) {

  val rExtractor = this

  def unapply(e : Expression) : Option[(String, DruidColumn, Option[String])] = e match {
    case ScalaUDF(fn, _, Seq(AttributeReference(nm, _, _, _)), _)
      if fn == dateTimeFn  => {
      val dC = dqb.druidColumn(nm)
      dC.filter(_.isDimension()).map((nm, _, None))
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
