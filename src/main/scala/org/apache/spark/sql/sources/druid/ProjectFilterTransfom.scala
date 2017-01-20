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

package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, LongType}
import org.apache.spark.sql.util.ExprUtil
import org.sparklinedata.druid.Debugging._
import org.sparklinedata.druid._
import org.sparklinedata.druid.jscodegen.JSCodeGenerator
import org.sparklinedata.druid.metadata._

trait ProjectFilterTransfom {
  self: DruidPlanner =>


  def addUnpushedAttributes(dqb : DruidQueryBuilder,
                            e : Expression,
                            isProjection : Boolean) :
  Option[DruidQueryBuilder] = {
    e.references.foldLeft(Some(dqb): Option[DruidQueryBuilder]){(odqb, a) =>
      val d = odqb.get
      odqb.flatMap(_.druidColumn(a.name)).map(_ => d)
    }.map { dqb =>
      if (isProjection) {
        dqb.copy(hasUnpushedProjections = true)
      } else {
        dqb.copy(hasUnpushedFilters = true)
      }
    }
  }

  def translateProjectFilter(dqb1 : Option[DruidQueryBuilder],
                             projectList : Seq[NamedExpression],
                             filters : Seq[Expression],
                            ignoreProjectList : Boolean = false,
                             joinAttrs : Set[String] = Set()) : Seq[DruidQueryBuilder] = {
    val dqb = if (ignoreProjectList) {
      dqb1
    } else {
      projectList.foldLeft(dqb1) { (dqB, e) =>
        dqB.flatMap(projectExpression(_, e, joinAttrs, ignoreProjectList))
      }
    }

    if (dqb.isDefined) {
      /*
       * Filter Rewrites:
       * - A conjunct is a predicate on the Time Dimension => rewritten to Interval constraint
       * - A expression containing comparisons on Dim Columns.
       */
      val iCE: IntervalConditionExtractor = new IntervalConditionExtractor(dqb.get)
      val iCE2: SparkIntervalConditionExtractor = new SparkIntervalConditionExtractor(dqb.get)
      var odqb = filters.foldLeft(dqb) { (dqB, e) =>
        dqB.flatMap { b =>
          intervalFilterExpression(b, iCE, iCE2, e).orElse(
            dimFilterExpression(b, e).map(p => b.filter(p)).orElse(
              addUnpushedAttributes(b, e, false)
            )
          )
        }
      }
      odqb = odqb.map { d =>
        d.copy(origProjList = d.origProjList.map(_ ++ projectList).orElse(Some(projectList))).
          copy(origFilter = d.origFilter.flatMap(o => ExprUtil.and(o +: filters)).
            orElse(ExprUtil.and(filters))
          )
      }
      odqb.debug.map(Seq(_)).getOrElse(Seq())
    } else Seq()
  }

  val druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l@LogicalRelation(d@DruidRelation(info, None), _))) => {
      val actualInfo = DruidMetadataCache.druidRelation(
        sqlContext,
        info
      )
      val dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(actualInfo))
      val sfe = ExprUtil.simplifyConjPred(dqb.get, filters)
      translateProjectFilter(Some(sfe._2),
        projectList,
        sfe._1)
    }
    case _ => Seq()
  }

  /**
   * For joins ignore projections at the individual table level. The projections above
   * the final join will be checked.
   */
  val druidRelationTransformForJoin: DruidTransform = {
    case (_, cacheTablePatternMatch(projectList, filters,
    l@LogicalRelation(d@DruidRelation(info, None), _))) => {
      val dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))
      translateProjectFilter(dqb,
        projectList,
        filters,
        true)
    }
    case _ => Seq()
  }

  def projectExpression(dqb: DruidQueryBuilder, pe: Expression,
                        joinAttrs: Set[String] = Set(),
                       ignoreProjectList : Boolean = false):
  Option[DruidQueryBuilder] = pe match {
    case _ if ignoreProjectList => Some(dqb)
    case AttributeReference(nm, dT, _, _) if dqb.druidColumn(nm).isDefined
    => Some(dqb)
    /*
     * If Attribute is a joining column and it is not in the DruidIndex,
     * then allow this. This is used when replacing a Dimension Table
     * join with a DriudQuery.
     */
    case AttributeReference(nm, dT, _, _) if joinAttrs.contains(nm)
    => Some(dqb)
    case Alias(ar@AttributeReference(nm1, dT, _, _), nm) => {
      for (dqbc <- projectExpression(dqb, ar, joinAttrs, ignoreProjectList))
        yield dqbc.addAlias(nm, nm1)
    }
    case _ => addUnpushedAttributes(dqb, pe, true)
  }

  def intervalFilterExpression(dqb: DruidQueryBuilder,
                               iCE: IntervalConditionExtractor,
                               iCE2 : SparkIntervalConditionExtractor,
                               fe: Expression):
  Option[DruidQueryBuilder] = fe match {
    case iCE(iC) => dqb.interval(iC)
    case iCE2(iC) => dqb.interval(iC)
    case _ => None
  }

  private def javascriptFilter(dC : DruidRelationColumn,
                       compOp : String,
                       value : Any) : FilterSpec = {

    JavascriptFilterSpec.create(dC.name, compOp, value.toString)
  }

  private def boundFilter(dC : DruidRelationColumn,
                  lowerValue : Option[Any],
                  lowerStrict : Boolean,
                  upperValue : Option[Any],
                  upperStrict : Boolean,
                  value : Any,
                  sparkDT : DataType
                 ) : FilterSpec = {

    val alphaNumeric = isNumericType(sparkDT)
    var f = new BoundFilterSpec(dC.name, None, None, None, None, alphaNumeric)
    if (lowerValue.isDefined) {
      f = f.copy(lower = Some(lowerValue.get.toString), lowerStrict = Some(lowerStrict))
    }
    if (upperValue.isDefined) {
      f = f.copy(upper = Some(upperValue.get.toString), upperStrict = Some(upperStrict))
    }
    f
  }

  private def compOp(dDS : DruidDataSource,
             dC : DruidRelationColumn,
             value : Any,
             sparkDT : DataType,
             compOp : String) : FilterSpec = {
    if (dDS.supportsBoundFilter) {
      compOp match {
        case "<" => boundFilter(dC, None, false, Some(value), true, value, sparkDT)
        case "<=" => boundFilter(dC, None, false, Some(value), false, value, sparkDT)
        case ">" => boundFilter(dC, Some(value), true, None, false, value, sparkDT)
        case ">=" => boundFilter(dC, Some(value), false, None, true, value, sparkDT)
        case _ => ???
      }
    } else {
      javascriptFilter(dC, compOp, value)
    }
  }

  object ValidDruidNativeComparison {

    def unapply(t : (DruidQueryBuilder, Expression)) : Option[FilterSpec] =  {
      val dqb = t._1
      val e = t._2
      import SparkNativeTimeElementExtractor._
      val timeRefExtractor = new SparkNativeTimeElementExtractor(dqb)
      e match {
        case EqualTo(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield new SelectorFilterSpec(dD.name, value.toString)
        }
        case EqualTo(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield new SelectorFilterSpec(dD.name, value.toString)
        }
        case LessThan(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  "<")
        }
        case LessThan(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  ">")
        }
        case LessThan(timeRefExtractor(dtGrp), Literal(value, LongType))
          if dtGrp.druidColumn.name != DruidDataSource.TIME_COLUMN_NAME &&
            dtGrp.formatToApply == TIMESTAMP_FORMAT =>
          None // TODO convert this to a JavascriptFilter ?
        // TODO handle all other comparision fns (lte, gt, gte, eq)
        case LessThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  "<=")
        }
        case LessThanOrEqual(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  ">=")
        }

        case GreaterThan(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  ">")
        }
        case GreaterThan(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  "<")
        }
        case GreaterThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  ">=")
        }
        case GreaterThanOrEqual(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.isDimension() && DruidDataType.sparkDataType(dD.dataType) == dT)
            yield compOp(dqb.drInfo.druidDS, dD, value, dT,  "<=")
        }
        case _ => None
      }
    }
  }

  object CompDetails {
    def unapply(e : Expression) : Option[(String, Any, DataType, String, DataType)] = e match {
      case GreaterThan(AttributeReference(nm, dT, _, _), Literal(value, lDT)) =>
        Some((">", value, lDT, nm, dT))
      case LessThan(Literal(value, lDT), AttributeReference(nm, dT, _, _)) =>
        Some((">", value, lDT, nm, dT))
      case GreaterThan(Literal(value, lDT), AttributeReference(nm, dT, _, _)) =>
        Some(("<", value, lDT, nm, dT))
      case LessThan(AttributeReference(nm, dT, _, _), Literal(value, lDT)) =>
        Some(("<", value, lDT, nm, dT))
      case GreaterThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, lDT)) =>
        Some((">=", value, lDT, nm, dT))
      case LessThanOrEqual(Literal(value, lDT), AttributeReference(nm, dT, _, _)) =>
        Some((">=", value, lDT, nm, dT))
      case GreaterThanOrEqual(Literal(value, lDT), AttributeReference(nm, dT, _, _)) =>
        Some(("<=", value, lDT, nm, dT))
      case LessThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, lDT)) =>
        Some(("<=", value, lDT, nm, dT))
      case _ => None
    }
  }

  object SpatialComparison {

    private def spatialBound(dRInfo : DruidRelationInfo,
                             dC : DruidRelationColumn,
                             value : Any,
                             sparkDT : DataType,
                             compOp : String) : FilterSpec = {

      val bound = value.toString.toDouble
      val spatialIndex = dRInfo.spatialIndexMap(dC.spatialIndex.get.druidColumn.name)
      val dim = dC.spatialIndex.get.spatialPosition
      val isMin = compOp == ">" || compOp == ">="
      val includeVal = compOp == ">=" || compOp == "<="
      SpatialFilterSpec(dC.name, spatialIndex.getBounds(dim, bound, isMin, includeVal))
    }

    def unapply(t: (DruidQueryBuilder, Expression)): Option[FilterSpec] = {
      val dqb = t._1
      val e = t._2
      e match {
        case CompDetails(compOp, value, lDT, nm, dT) => {
          for (dD <- dqb.druidColumn(nm)
               if dD.hasSpatialIndex && DoubleType.acceptsType(lDT) &&
                 DruidDataType.sparkDataType(dD.dataType) == dT)
            yield spatialBound(dqb.drInfo, dD, value, dT, compOp)
        }
        case _ => None
      }

    }
  }

  def dimFilterExpression(dqb: DruidQueryBuilder, fe: Expression):
  Option[FilterSpec] = {

    val dtTimeCond = new DateTimeConditionExtractor(dqb)
    val timeRefExtractor = new SparkNativeTimeElementExtractor(dqb)

    (dqb, fe) match {
      case ValidDruidNativeComparison(filSpec) => Some(filSpec)
      case SpatialComparison(filSpec) => Some(filSpec)
      case (dqb, fe) => fe match {
        case dtTimeCond((dCol, op, value)) =>
          Some(JavascriptFilterSpec.create(dCol, op, value))
        case Or(e1, e2) => {
          Utils.sequence(
            List(dimFilterExpression(dqb, e1), dimFilterExpression(dqb, e2))).map { args =>
            LogicalFilterSpec("or", args.toList)
          }
        }
        case And(e1, e2) => {
          Utils.sequence(
            List(dimFilterExpression(dqb, e1), dimFilterExpression(dqb, e2))).map { args =>
            LogicalFilterSpec("and", args.toList)
          }
        }
        case In(AttributeReference(nm, dT, _, _), vl: Seq[Expression]) => {
          for (dD <- dqb.druidColumn(nm) if dD.isDimension() &&
            (vl.forall(e => e.isInstanceOf[Literal])))
            yield new ExtractionFilterSpec(dD.name, (for (e <- vl) yield e.toString()).toList)
        }
        case InSet(AttributeReference(nm, dT, _, _), vl: Set[Any]) => {
          val primitieVals = vl.foldLeft(true)((x, y) =>
            x & (y.isInstanceOf[Literal] || !y.isInstanceOf[Expression]))
          for (dD <- dqb.druidColumn(nm) if dD.isDimension() && primitieVals)
            yield new ExtractionFilterSpec(dD.name, (for (e <- vl) yield e.toString()).toList)
        }
        case IsNotNull(AttributeReference(nm, _, _, _)) => {
          for (c <- dqb.druidColumn(nm) if c.isDimension()) yield
            NotFilterSpec("not", new SelectorFilterSpec(nm, ""))
        }
        // TODO: turn isnull(TimeDim/Metric) to NULL SCAN
        case IsNull(AttributeReference(nm, _, _, _)) => {
          for (c <- dqb.druidColumn(nm)
               if c.isDimension()) yield
            new SelectorFilterSpec(c.name, "")
        }
        case Not(e) => {
          val fil = dimFilterExpression(dqb, e)
          for (f <- fil)
            yield NotFilterSpec("not", f)
        }
        // TODO: Hack till we do NULL SCAN
        case Literal(null, _) => Some(new SelectorFilterSpec("__time", ""))
        case _ => {
          val codeGen = JSCodeGenerator(dqb, fe, false, false,
            sqlContext.getConf(DruidPlanner.TZ_ID).toString,
            BooleanType)
          for (fn <- codeGen.fnCode) yield {
            new JavascriptFilterSpec(codeGen.fnParams.last, fn)
          }
        }
      }
    }
  }
}

