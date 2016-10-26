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

import org.apache.spark.sql.types._
import org.sparklinedata.druid.jscodegen.JSDateTimeCtx._

// TODO: Track nullability of each JSEXpr & and add null safe casting if rqd.
//       Metrics & Time DIM can't be null; Non TIME_DIM DIM or VCOLS of Metric/TIME_DIM
//       using conditional exprs can have NULL values.

case class JSCast(from: JSExpr, toDT: DataType, ctx: JSCodeGenerator) {
  private[jscodegen] val castCode: Option[JSExpr] =
    toDT match {
      case _ if ((toDT == from.fnDT && (!from.timeDim || toDT != StringType)) ||
        from.fnDT == NullType) =>
        Some(JSExpr(None,from.linesSoFar, from.getRef, StringType))
      case BooleanType => castToBooleanCode
      case ShortType => castToShortCode
      case IntegerType => castToNumericCode(IntegerType)
      case FloatType => castToNumericCode(FloatType)
      case LongType => castToNumericCode(LongType)
      case DoubleType => castToNumericCode(DoubleType)
      case StringType => castToStringCode
      case DateType => castToDateCode
      case TimestampType => castToTimestampCode
      case _ => None
    }

  private[this] def castToBooleanCode: Option[JSExpr] = from.fnDT match {
    case StringType | IntegerType | LongType | FloatType | DoubleType =>
      Some(JSExpr(None, from.linesSoFar, s"Boolean(${from.getRef})", BooleanType))
    case DateType =>
      // Hive would return null when cast from date to boolean
      // TODO: review this; Shouldn't this be "" ?
      Some(JSExpr(None, from.linesSoFar, s"null", BooleanType))
    case TimestampType =>
      Some(JSExpr(None, from.linesSoFar, s"Boolean(${from.getRef}.getMillis())", BooleanType))
    case _ => None
  }

  // TODO: enable support for Decimal (if Decimal is not represented by Number)
  private[this] def castToNumericCode(outDt: DataType): Option[JSExpr] =
    from.fnDT match {
      case BooleanType | StringType =>
        Some(JSExpr(None, from.linesSoFar, s"Number(${from.getRef})", outDt))
      case (FloatType | DoubleType) if ctx.SPIntegralNumeric(outDt) =>
        Some (JSExpr(None, from.linesSoFar, s"Math.round(${from.getRef})", outDt))
      case ShortType | IntegerType | LongType| FloatType | DoubleType  =>
        Some (JSExpr(None, from.linesSoFar, from.getRef, outDt))
      case DateType =>
        Some(JSExpr(None, from.linesSoFar, s"null", outDt))
      case TimestampType =>
        Some(JSExpr(None, from.linesSoFar, s"Number(${dtToLongCode(from.getRef)})", outDt))
      case _ => None
    }

  // TODO: enable Cast To Short after support for Decimal, Short, TimeStamp Types
  private[this] def castToShortCode: Option[JSExpr] = from.fnDT match {
    case StringType | BooleanType | DateType => castToNumericCode(ShortType)
    case _ => None
  }

  private[this] def castToStringCode: Option[JSExpr] = from.fnDT match {
    case ShortType | IntegerType | LongType | FloatType | DoubleType | DecimalType() =>
      nullSafeCastToString(from.getRef)
    case DateType =>
      nullSafeCastToString(dateToStrCode(from.getRef))
    case TimestampType =>
      nullSafeCastToString(dtToStrCode(from.getRef))
    case StringType if from.timeDim =>
      nullSafeCastToString(dtToStrCode(longToISODTCode(from.getRef, ctx.dateTimeCtx)))
    case _ => None
  }

  // TODO: Handle parsing failure as in Spark
  private[this] def castToDateCode: Option[JSExpr] = from.fnDT match {
    case StringType =>
      Some(JSExpr(None, from.linesSoFar,
        if (from.timeDim) {longToDateCode(from.getRef, ctx.dateTimeCtx)}
      else {stringToDateCode(from.getRef, ctx.dateTimeCtx)}, DateType))
    case TimestampType =>
      Some(JSExpr(None, from.linesSoFar, dtToDateCode(from.getRef), DateType))
    case _ => None
  }

  // TODO: Support for DecimalType. Handle Double/Float isNaN/isInfinite
  private[this] def castToTimestampCode: Option[JSExpr] = from.fnDT match {
    case StringType =>
      Some(JSExpr(None, from.linesSoFar,
        if (from.timeDim) {longToISODTCode(from.getRef, ctx.dateTimeCtx)}
        else {stringToISODTCode(from.getRef, ctx.dateTimeCtx)}, TimestampType))
    case BooleanType =>
      Some(JSExpr(None, from.linesSoFar, stringToISODTCode
      (s"((${from.getRef}) == true ? T00:00:01Z : T00:00:00Z)", ctx.dateTimeCtx),
        TimestampType))
    case FloatType | DoubleType | DecimalType() =>
      for (le <- castToNumericCode(LongType)) yield
        JSExpr(None, le.linesSoFar, longToISODTCode(le.getRef, ctx.dateTimeCtx),
          TimestampType)
    case ShortType | IntegerType | LongType =>
      Some(JSExpr(None, from.linesSoFar,
        longToISODTCode(from.getRef, ctx.dateTimeCtx), TimestampType))
    case DateType => Some(JSExpr(None, from.linesSoFar,
      localDateToDTCode(from.getRef, ctx.dateTimeCtx), TimestampType))
    case _ => None
  }

  private[this] def nullSafeCastToString(valToCast: String): Option[JSExpr] = {
    if (from.fnVar.isEmpty) {
      val v1 = ctx.makeUniqueVarName
      Some(JSExpr(None, from.linesSoFar + s"var $v1 = $valToCast;",
        s"""(($v1 != null) ? $v1.toString() : "")""".stripMargin, StringType))
    } else {
      Some(JSExpr(None, from.linesSoFar,
        s"""(($valToCast != null) ? $valToCast.toString() : "")""".stripMargin,
        StringType))
    }
  }
}