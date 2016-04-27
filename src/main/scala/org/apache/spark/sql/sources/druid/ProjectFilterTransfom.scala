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
import org.apache.spark.sql.types.LongType
import org.sparklinedata.druid.Debugging._
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.{DruidDataSource, DruidDimension}

trait ProjectFilterTransfom {
  self: DruidPlanner =>

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
      filters.foldLeft(dqb) { (dqB, e) =>
        dqB.flatMap { b =>
          intervalFilterExpression(b, iCE, iCE2, e).orElse(
            dimFilterExpression(b, e).map(p => b.filter(p))
          )
        }
      }.debug.map(Seq(_)).getOrElse(Seq())
    } else Seq()
  }

  val druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l@LogicalRelation(d@DruidRelation(info, None), _))) => {
      val dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))
      translateProjectFilter(dqb,
        projectList,
        filters)
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
    case _ => None
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

  def dimFilterExpression(dqb: DruidQueryBuilder, fe: Expression):
  Option[FilterSpec] = {

    import SparkNativeTimeElementExtractor._
    val dtTimeCond = new DateTimeConditionExtractor(dqb)
    val timeRefExtractor = new SparkNativeTimeElementExtractor(dqb)

    fe match {
      case EqualTo(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield new SelectorFilterSpec(dD.name, value.toString)
      }
      case EqualTo(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield new SelectorFilterSpec(dD.name, value.toString)
      }
      case LessThan(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<", value.toString)
      }
      case LessThan(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">", value.toString)
      }
      case LessThan(timeRefExtractor(dtGrp), Literal(value, LongType))
        if dtGrp.druidColumn.name != DruidDataSource.TIME_COLUMN_NAME &&
          dtGrp.formatToApply == TIMESTAMP_FORMAT =>
        None // TODO convert this to a JavascriptFilter ?
        // TODO handle all other comparision fns (lte, gt, gte, eq)
      case LessThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<=", value.toString)
      }
      case LessThanOrEqual(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">=", value.toString)
      }

      case GreaterThan(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">", value.toString)
      }
      case GreaterThan(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<", value.toString)
      }
      case GreaterThanOrEqual(AttributeReference(nm, dT, _, _), Literal(value, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, ">=", value.toString)
      }
      case GreaterThanOrEqual(Literal(value, _), AttributeReference(nm, dT, _, _)) => {
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension])
          yield JavascriptFilterSpec.create(dD.name, "<=", value.toString)
      }
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
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension] &&
          (vl.forall(e => e.isInstanceOf[Literal])))
          yield new ExtractionFilterSpec(dD.name, (for (e <- vl) yield e.toString()).toList)
      }
      case InSet(AttributeReference(nm, dT, _, _), vl: Set[Any]) => {
        val primitieVals = vl.foldLeft(true)((x,y) =>
          x & ( y.isInstanceOf[Literal] || !y.isInstanceOf[Expression]))
        for (dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension] && primitieVals)
          yield new ExtractionFilterSpec(dD.name, (for (e <- vl) yield e.toString()).toList)
      }
      case Not(e) => {
        val fil = dimFilterExpression(dqb, e)
        for (f <- fil)
          yield NotFilterSpec("not", f)
      }
      case _ => None
    }
  }
}

