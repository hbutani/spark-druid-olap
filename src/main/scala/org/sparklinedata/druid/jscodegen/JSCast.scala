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

case class JSCast(from: JSExpr, to: DataType, ctx: JSCodeGenerator) {
  private[jscodegen] val castCode: Option[JSExpr] =
    to match {
      case _ if (to == from.fnDT || from.fnDT == NullType) =>
        Some(new JSExpr(from.getRef, StringType))
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
      Some(new JSExpr(s"Boolean(f${from.getRef})", BooleanType))
    case DateType =>
      // Hive would return null when cast from date to boolean
      Some(new JSExpr(s"null", BooleanType))
    case TimestampType =>
      Some(new JSExpr(s"Boolean(${from.getRef}.getMillis())", BooleanType))
    case _ => None
  }


  // TODO: enable support for Decimal (if Decimal is not represented by Number)
  private[this] def castToNumericCode(outDt: DataType): Option[JSExpr] =
    from.fnDT match {
      case BooleanType | StringType =>
        Some(new JSExpr(s"Number(${from.getRef})", outDt))
      case (FloatType | DoubleType) if ctx.SPIntegralNumeric(outDt) =>
        Some (new JSExpr(s"Math.round(${from.getRef})", outDt))
      case ShortType | IntegerType | LongType| FloatType | DoubleType  =>
        Some (new JSExpr(from.getRef, outDt))
      case DateType =>
        Some(new JSExpr(s"null", outDt))
      case TimestampType =>
        Some(new JSExpr(s"Number(${dtToLongCode(from.getRef)})", outDt))
      case _ => None
    }

  // TODO: enable Cast To Short after support for Decimal, Short, TimeStamp Types
  private[this] def castToShortCode: Option[JSExpr] = from.fnDT match {
    case StringType | BooleanType | DateType => castToNumericCode(ShortType)
    case _ => None
  }

  private[this] def castToStringCode: Option[JSExpr] = from.fnDT match {
      case ShortType | IntegerType | LongType | FloatType | DoubleType | DecimalType() =>
        Some(new JSExpr(s"${from.getRef}.toString()", StringType))
      case DateType =>
        Some(new JSExpr(dateToStrCode(from.getRef), StringType))
      case TimestampType =>
        Some(new JSExpr(dtToStrCode(from.getRef), StringType))
      case _ => None
    }

  // TODO: Handle parsing failure as in Spark
  private[this] def castToDateCode: Option[JSExpr] = from.fnDT match {
    case StringType =>
      Some(new JSExpr(if (from.timeDim) {longToDateCode(from.getRef, ctx.dateTimeCtx)}
      else {stringToDateCode(from.getRef, ctx.dateTimeCtx)}, DateType))
    case TimestampType =>
      Some(new JSExpr(dtToDateCode(from.getRef), DateType))
    case _ => None
  }

  // TODO: Support for DecimalType. Handle Double/Float isNaN/isInfinite
  private[this] def castToTimestampCode: Option[JSExpr] = from.fnDT match {
    case StringType =>
      Some(new JSExpr(if (from.timeDim) {longToISODTCode(from.getRef, ctx.dateTimeCtx)}
      else {stringToISODTCode(from.getRef, ctx.dateTimeCtx)}, TimestampType))
    case BooleanType =>
      Some(new JSExpr(stringToISODTCode
      (s"((${from.getRef}) == true ? T00:00:01Z : T00:00:00Z)", ctx.dateTimeCtx),
        TimestampType))
    case FloatType | DoubleType | DecimalType() =>
      for (le <- castToNumericCode(LongType)) yield
        JSExpr(None, le.linesSoFar, longToISODTCode(le.getRef, ctx.dateTimeCtx),
          TimestampType)
    case ShortType | IntegerType | LongType =>
      Some(new JSExpr(longToISODTCode(from.getRef, ctx.dateTimeCtx), TimestampType))
    case DateType => Some(new JSExpr(localDateToDTCode(from.getRef, ctx.dateTimeCtx),
      TimestampType))
    case _ => None
  }
}