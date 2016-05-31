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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.sparklinedata.druid.DruidQueryBuilder
import org.sparklinedata.druid.jscodegen.JSDateTimeCtx._
import org.sparklinedata.druid.metadata._

import scala.collection.mutable
import scala.language.reflectiveCalls


case class JSCodeGenerator(dqb: DruidQueryBuilder, e: Expression, mulInParamsAllowed: Boolean,
                           metricAllowed: Boolean, tz_id: String,
                           retType: DataType = StringType) extends Logging {
  private[this] var uid: Int = 0
  private[jscodegen] def makeUniqueVarName: String = {
    uid += 1
    "v" + uid
  }
  private[this] var inParams: mutable.HashSet[String] = mutable.HashSet()
  private[jscodegen] val dateTimeCtx = new JSDateTimeCtx(tz_id, this)

  private[jscodegen] def fnElements: Option[(String, String)] =
    for (fnb <- genExprCode(e) if inParams.nonEmpty;
         rStmt <- JSCast(fnb, retType, this).castCode) yield {
      (s"""
            ${dateTimeCtx.dateTimeInitCode}
            ${fnb.linesSoFar}
            ${rStmt.linesSoFar}""".stripMargin, rStmt.getRef)
    }

  def fnCode: Option[String] =
    for (fne <- fnElements) yield {
      s"""function (${fnParams.mkString(", ")}) {
            ${fne._1}

            return(${fne._2});
            }""".stripMargin
    }

  def fnParams = inParams.toList


  private[jscodegen] def SPIntegralNumeric(t: DataType): Boolean = t match {
    case ShortType | IntegerType | LongType => true
    case _ => false
  }

  private[this] def genExprCode(e: Expression): Option[JSExpr] = {
    e match {
      case AttributeReference(nm, dT, _, _) =>
        for (dD <- dqb.druidColumn(nm);
             v = dqb.druidColumn(nm).get.name
             if ((dD.isInstanceOf[DruidTimeDimension] ||
               (metricAllowed && dD.isInstanceOf[DruidMetric]) ||
               dD.isInstanceOf[DruidDimension]) && validInParams(v))) yield {
          new JSExpr(v, e.dataType, dD.isInstanceOf[DruidTimeDimension])
        }

      case Literal(value, dataType) => {
        dataType match {
          case IntegerType | LongType | ShortType | DoubleType | FloatType =>
            Some(new JSExpr(value.toString, dataType))
          case StringType => Some(new JSExpr(s""""${e.toString}"""", dataType))
          case NullType => Some(new JSExpr("null", dataType))
          case DateType if value.isInstanceOf[Int] =>
            Some(new JSExpr(noDaysToDateCode(value.toString), dataType))
          case TimestampType if value.isInstanceOf[Long] =>
            Some(new JSExpr(longToISODTCode(value.asInstanceOf[Long], dateTimeCtx), dataType))
          case _ => JSCast(
            new JSExpr(value.toString, StringType), dataType, this).castCode
        }
      }

      case Cast(s, dt) => Some(genCastExprCode(s, dt)).flatten

      case Concat(eLst) =>
        // TODO: If second gencode failed then this should return none
        // TODO: Use FoldLeft
        var cJSFn: Option[JSExpr] = None
        for (ex <- eLst; exCode <- genExprCode(ex)) {
          if (cJSFn.isEmpty) {
            cJSFn = Some(exCode)
          } else {
            cJSFn = Some(JSExpr(None,
              s"${cJSFn.get.linesSoFar} ${exCode.linesSoFar} ",
              s"((${cJSFn.get.getRef}).concat(${exCode.getRef}))",
              StringType))
          }
        }
        cJSFn
      case Upper(u) =>
        for (exCode <- genExprCode(u) if u.dataType.isInstanceOf[StringType]) yield
          JSExpr(None,
            exCode.linesSoFar, s"(${exCode.getRef}).toUpperCase()", StringType)
      case Lower(l) =>
        for (exCode <- genExprCode(l) if l.dataType.isInstanceOf[StringType]) yield
          JSExpr(None,
            exCode.linesSoFar, s"(${exCode.getRef}).toLowerCase()", StringType)
      case Substring(str, pos, len) =>
        for (strcode <- genExprCode(str); posL <- genExprCode(pos)
             if pos.isInstanceOf[Literal]; lenL <- genExprCode(len)
             if len.isInstanceOf[Literal]) yield
          JSExpr(None, s"${strcode.linesSoFar} ${posL.linesSoFar}",
            s"(${strcode.getRef}).substr(${posL.getRef}, ${lenL.getRef})",
            StringType)
      case Coalesce(le) => {
        val l = le.flatMap(e => genExprCode(e))
        if (le.size == l.size) {
          val cl = l.foldLeft(new JSExpr("", StringType))((a, j) =>
            JSExpr(None,
              a.linesSoFar + j.linesSoFar,
              if (a.getRef.isEmpty) s"(${j.getRef})" else s"${a.getRef} || (${j.getRef})", j.fnDT))
          Some(JSExpr(None, cl.linesSoFar, s"(${cl.getRef})", cl.fnDT))
        } else {
          None
        }
      }

      case CaseWhen(le) => {
        val l = le.flatMap(e => genExprCode(e))
        if (l.length == le.length) {
          val v1 = makeUniqueVarName
          val jv = ((0 until l.length / 2).foldLeft(
            JSExpr(Some(v1), s"var ${v1} = false;", "", BooleanType)) { (a, i) =>
            val cond = l(i * 2)
            val res = l(i * 2 + 1)
            JSExpr(a.fnVar, a.linesSoFar +
              s"""
                  ${cond.linesSoFar}
              if (!${v1} && (${cond.getRef})) {
              ${res.linesSoFar}
              ${v1} = ${res.getRef};
        }""".stripMargin, "", a.fnDT)
          })
          val de = l(l.length - 1)
          Some(JSExpr(jv.fnVar, jv.linesSoFar +
            s"""
              if (!${v1}) {
              ${de.linesSoFar}
              ${v1} = ${de.getRef};
        }""".stripMargin, "", de.fnDT))
        } else {
          None
        }
      }
      case LessThan(l, r) => genComparisonCode(l, r, " < ")
      case LessThanOrEqual(l, r) => genComparisonCode(l, r, " <= ")
      case GreaterThan(l, r) => genComparisonCode(l, r, " > ")
      case GreaterThanOrEqual(l, r) => genComparisonCode(l, r, " >= ")
      case EqualTo(l, r) => genComparisonCode(l, r, " == ")
      case And(l, r) => genComparisonCode(l, r, " && ")
      case Or(l, r) => genComparisonCode(l, r, " || ")
      case Not(e) => {
        for (j <- genExprCode(e)) yield
          JSExpr(None, j.linesSoFar, s"!(${j.getRef})", BooleanType)
      }

      case ToDate(de) =>
        for (fn <- genExprCode(de); cFn <- JSCast(fn, DateType, this).castCode)
          yield JSExpr(None, fn.linesSoFar + cFn.linesSoFar, cFn.getRef, DateType)
      case DateAdd(sd, nd) =>
        for (sde <- genExprCode(sd); csde <- JSCast(sde, DateType, this).castCode;
             nde <- genExprCode(nd); cnde <- JSCast(nde, IntegerType, this).castCode;
             ade <- JSCast(new JSExpr(dateAdd(csde.getRef, cnde.getRef), DateType),
               StringType, this).castCode)
          yield JSExpr(None,
            sde.linesSoFar + csde.linesSoFar + nde.linesSoFar + cnde.linesSoFar + ade.linesSoFar,
            ade.getRef, StringType)
      case DateSub(sd, nd) =>
        for (sde <- genExprCode(sd); csde <- JSCast(sde, DateType, this).castCode;
             nde <- genExprCode(nd); cnde <- JSCast(nde, IntegerType, this).castCode)
          yield JSExpr(None,
            sde.linesSoFar + csde.linesSoFar + nde.linesSoFar + cnde.linesSoFar,
            dateSub(csde.getRef, cnde.getRef), DateType)
      case DateDiff(ed, sd) =>
        for (ede <- genExprCode(ed); cede <- JSCast(ede, DateType, this).castCode;
             sde <- genExprCode(sd); csde <- JSCast(sde, DateType, this).castCode)
          yield JSExpr(None,
            ede.linesSoFar + cede.linesSoFar + sde.linesSoFar + csde.linesSoFar,
            dateDiff(cede.getRef, csde.getRef), IntegerType)
      case Year(y) =>
        for (ye <- genExprCode(y); cye <- JSCast(ye, DateType, this).castCode) yield
          JSExpr(None, ye.linesSoFar + cye.linesSoFar, dTYear(cye.getRef), IntegerType)
      case Quarter(q) =>
        for (qe <- genExprCode(q); cqe <- JSCast(qe, DateType, this).castCode) yield
          JSExpr(None, qe.linesSoFar + cqe.linesSoFar, dTQuarter(cqe.getRef), IntegerType)
      case Month(mo) =>
        for (moe <- genExprCode(mo); cmoe <- JSCast(moe, DateType, this).castCode)
          yield
            JSExpr(None, moe.linesSoFar + cmoe.linesSoFar, dTMonth(cmoe.getRef), IntegerType)
      case DayOfMonth(d) =>
        for (de <- genExprCode(d); cde <- JSCast(de, DateType, this).castCode)
          yield
            JSExpr(None, de.linesSoFar + cde.linesSoFar, dTDayOfMonth(cde.getRef), IntegerType)
      case WeekOfYear(w) =>
        for (we <- genExprCode(w); cwe <- JSCast(we, DateType, this).castCode)
          yield
            JSExpr(None, we.linesSoFar + cwe.linesSoFar, dTWeekOfYear(cwe.getRef), IntegerType)
      case Hour(h) =>
        for (he <- genExprCode(h); che <- JSCast(he, TimestampType, this).castCode)
          yield
            JSExpr(None, he.linesSoFar + che.linesSoFar, dTHour(che.getRef), IntegerType)
      case Minute(m) =>
        for (me <- genExprCode(m); cme <- JSCast(me, TimestampType, this).castCode)
          yield
            JSExpr(None, me.linesSoFar + cme.linesSoFar, dTMinute(cme.getRef), IntegerType)
      case Second(s) =>
        for (se <- genExprCode(s); cse <- JSCast(se, TimestampType, this).castCode)
          yield
            JSExpr(None, se.linesSoFar + cse.linesSoFar, dTSecond(cse.getRef), IntegerType)
      case FromUnixTime(s, f) if f.isInstanceOf[Literal] =>
        for (se <- genExprCode(s); cse <- JSCast(se, LongType, this).castCode) yield
          JSExpr(None, se.linesSoFar + cse.linesSoFar,
            dtToStrCode(longToISODTCode(cse.getRef, dateTimeCtx),
              s""""${f.toString()}"""".stripMargin), StringType)
      case UnixTimestamp(t, f) if f.isInstanceOf[Literal] =>
        for (te <- genExprCode(t)) yield
          JSExpr(None, te.linesSoFar,
            dtToSecondsCode(stringToISODTCode(te.getRef, dateTimeCtx)), LongType)
      case Add(l, r) => Some(genBArithmeticExprCode(l, r, e, "+")).flatten
      case Subtract(l, r) => Some(genBArithmeticExprCode(l, r, e, "-")).flatten
      case Multiply(l, r) => Some(genBArithmeticExprCode(l, r, e, "*")).flatten
      case Divide(l, r) => for (ae <- genBArithmeticExprCode(l, r, e, "/")) yield
        {
          if (SPIntegralNumeric(e.dataType)) {
            JSExpr(None, ae.linesSoFar, s"Math.floor(${ae.getRef})", e.dataType)
          } else {
            ae
          }
        }
      case Remainder(l, r) => Some(genBArithmeticExprCode(l, r, e, "%")).flatten
      case Pmod(le, re) =>
      {
        for (lex <- genExprCode(le); rex <- genExprCode(re)) yield {
          val v = makeUniqueVarName
          JSExpr(Some(v),
            lex.linesSoFar + rex.linesSoFar +
            s"""var ${v} = 0;
                if (${lex.getRef} < 0) {
                |${v} = (${lex.getRef} + ${rex.getRef}) % ${rex.getRef};
                } else {
                  ${v} =${lex.getRef} % ${rex.getRef};
                }""".stripMargin, "", e.dataType)
          }
      }
      case Abs(ve) => genUnaryExprCode(ve, "Math.abs")
      case Floor(ve) => genUnaryExprCode(ve, "Math.floor")
      case Ceil(ve) => genUnaryExprCode(ve, "Math.ceil")
      case Sqrt(ve) => genUnaryExprCode(ve, "Math.sqrt")
      case Logarithm(Literal(2.718281828459045, DoubleType), ve) => genUnaryExprCode(ve, "Math.log")
      case UnaryMinus(ve) => genUnaryExprCode(ve, "-")
      case UnaryPositive(ve) => genUnaryExprCode(ve, "+")
      case _ => None.flatten
    }
  }

  private[this] def genBArithmeticExprCode(l: Expression, r: Expression, ce: Expression,
                                           op: String): Option[JSExpr] = {
    for (lc <- genExprCode(l); rc <- genExprCode(r)) yield
      JSExpr(None,
        s"${lc.linesSoFar} ${rc.linesSoFar} ",
        s"((${lc.getRef}) $op (${rc.getRef}))", ce.dataType)
  }

  private[this] def genUnaryExprCode(e: Expression, op: String): Option[JSExpr] = {
    for (te <- genExprCode(e)) yield
      JSExpr(None, te.linesSoFar,
        s"""$op(${te.getRef})""".stripMargin, te.fnDT)
  }

  private[this] def genComparisonCode(l: Expression, r: Expression, op: String): Option[JSExpr] = {
    for (le <- genExprCode(l); re <- genExprCode(r);
         cstr <- genComparisonStr(l, le, r, re, op)) yield {
      JSExpr(le.fnVar.filter(_.nonEmpty).orElse(re.fnVar),
        le.linesSoFar + re.linesSoFar,
        cstr, BooleanType)
    }
  }

  /**
    * Generate comparison string for various left/right data types.
    * Assumption: Spark will make sure both left and right is same type family.
    *
    * @param l
    * @param ljs
    * @param r
    * @param rjs
    * @param op
    * @return
    */
  private[this] def genComparisonStr(l: Expression, ljs: JSExpr,
                                     r: Expression, rjs: JSExpr, op: String): Option[String] = {
    if (l.dataType.isInstanceOf[DateType] || l.dataType.isInstanceOf[TimestampType] ||
      r.dataType.isInstanceOf[DateType] || r.dataType.isInstanceOf[TimestampType]) {
      dComparison(ljs.getRef, rjs.getRef, op)
    } else  {
      Some(s"""(${ljs.getRef}) ${op} (${rjs.getRef})""".stripMargin)
    }
  }

  private[this] def genCastExprCode(e: Expression, dt: DataType): Option[JSExpr] = {
    for (fn <- genExprCode(simplifyCast(e, dt));
         cs <- JSCast(fn, dt, this).castCode) yield
      JSExpr(cs.fnVar, fn.linesSoFar + cs.linesSoFar, cs.curLine, dt)
  }

  private[this] def validInParams(inParam: String): Boolean = {
    inParams += inParam
    if (!mulInParamsAllowed && (inParams.size > 1)) false else true
  }

  private[this] def simplifyCast(e: Expression, edt: DataType): Expression = e match {
    case Cast(ie, idt) if edt.isInstanceOf[NumericType] && (idt.isInstanceOf[DoubleType] ||
      idt.isInstanceOf[FloatType] || idt.isInstanceOf[DecimalType]) =>
      Cast(ie, edt)
    case _ => e
  }
}