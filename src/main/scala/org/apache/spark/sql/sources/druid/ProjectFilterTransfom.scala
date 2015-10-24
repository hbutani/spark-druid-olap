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
import org.apache.spark.sql.sources.LogicalRelation
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.DruidDimension

trait ProjectFilterTransfom {
  self: DruidPlanner =>

  def translateProjectFilter(dqb1 : Option[DruidQueryBuilder],
                             projectList : Seq[NamedExpression],
                             filters : Seq[Expression],
                             joinAttrs : Set[String] = Set()) : Seq[DruidQueryBuilder] = {

    val dqb = projectList.foldLeft(dqb1) { (dqB, e) =>
      dqB.flatMap(projectExpression(_, e, joinAttrs))
    }

    if (dqb.isDefined) {
      /*
       * Filter Rewrites:
       * - A conjunct is a predicate on the Time Dimension => rewritten to Interval constraint
       * - A expression containing comparisons on Dim Columns.
       */
      val iCE: IntervalConditionExtractor = new IntervalConditionExtractor(dqb.get)
      filters.foldLeft(dqb) { (dqB, e) =>
        dqB.flatMap { b =>
          intervalFilterExpression(b, iCE, e).orElse(
            dimFilterExpression(b, e).map(p => b.filter(p))
          )
        }
      }.map(Seq(_)).getOrElse(Seq())
    } else Seq()
  }

  val druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l@LogicalRelation(d@DruidRelation(info, None)))) => {
      val dqb: Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))
      translateProjectFilter(dqb,
        projectList,
        filters)
    }
    case _ => Seq()
  }

  def projectExpression(dqb: DruidQueryBuilder, pe: Expression,
                        joinAttrs: Set[String] = Set()):
  Option[DruidQueryBuilder] = pe match {
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
      for (dqbc <- projectExpression(dqb, ar))
        yield dqbc.addAlias(nm, nm1)
    }
    case _ => None
  }

  def intervalFilterExpression(dqb: DruidQueryBuilder,
                               iCE: IntervalConditionExtractor, fe: Expression):
  Option[DruidQueryBuilder] = fe match {
    case iCE(iC) => dqb.interval(iC)
    case _ => None
  }

  def dimFilterExpression(dqb: DruidQueryBuilder, fe: Expression):
  Option[FilterSpec] = {

    val dtTimeCond = new DateTimeConditionExtractor(dqb)

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
      case _ => None
    }
  }

}

