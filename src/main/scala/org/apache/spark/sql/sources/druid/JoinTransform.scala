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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression, Expression}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ExtractEquiJoinKeys}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.sparklinedata.druid.DruidQueryBuilder
import org.sparklinedata.druid.metadata.StarSchema

case class DimTableInfo(projectList: Seq[NamedExpression],
                        filters: Seq[Expression],
                        relation: BaseRelation,
                        joinExpressions: Seq[Expression]) {

  lazy val joinAttrs: Set[String] = joinExpressions.map {
    case AttributeReference(nm, _, _, _) => nm
  }.toSet
}

sealed trait JoinNode {

  val leftExpressions: Seq[Expression]
  val rightExpressions: Seq[Expression]

  private def checkStarJoin(tables: Map[String, DimTableInfo])
                           (implicit sSchema: StarSchema): Either[String, Map[String, DimTableInfo]] = {
    (sSchema.isStarJoin(leftExpressions, rightExpressions), this) match {
      case (None, _) =>
        Left(s"Join condition is not a Star Schema " +
          s"Join: ${leftExpressions.zip(rightExpressions).mkString(", ")}")
      case (Some((l, r)), self: LeafJoinNode) => {
        if (tables.contains(l) && tables(l) != self.leftDimInfo) {
          Left(s"multiple join paths to table '${l}; this is not allowed")
        } else if (tables.contains(r) && tables(r) != self.rightDimInfo) {
          Left(s"multiple join paths to table '${r}; this is not allowed")
        } else {
          Right(tables ++ Map(l -> self.leftDimInfo, r -> self.rightDimInfo))
        }
      }
      case (Some((l, r)), _) => {
        Right(tables)
      }
    }
  }

  private def checkOnesidedJoinTree(jTree: JoinNode)
                                   (implicit sSchema: StarSchema):
  Either[String, Map[String, DimTableInfo]] = {
    val lJPath = jTree.validate(sSchema)
    lJPath.right.flatMap { tables =>
      checkStarJoin(tables)
    }
  }

  /**
   * Ensure that this JoinTree is a subGraph of the StarSchema's join tree.
   * - every join must be a [[StarSchema.isStarJoin valid star join]]
   * - a StarSchema table must appear only once in the JoinTree
   * @param sSchema
   * @return
   */
  def validate(implicit sSchema: StarSchema):
  Either[String, Map[String, DimTableInfo]] = this match {
    case n: LeafJoinNode => checkStarJoin(Map())
    case LeftJoinNode(left, lE, rE, _) => checkOnesidedJoinTree(left)
    case RightJoinNode(right, lE, rE, _) => checkOnesidedJoinTree(right)
    case BushyJoinNode(left, right, _, _) => {
      val lJPath = left.validate(sSchema)
      lJPath.right.flatMap { ltables =>
        val rJPath = right.validate(sSchema)
        rJPath.right.flatMap { rtables =>
          val dupTables = ltables.keys.toSet.intersect(rtables.keys.toSet)
          if (!dupTables.isEmpty) {
            Left(s"multiple join paths to tables: ${dupTables.mkString(",")}")
          } else {
            val tables = ltables ++ rtables
            checkStarJoin(tables)
          }
        }
      }
    }
  }
}

case class LeafJoinNode(
                         leftExpressions: Seq[Expression],
                         leftDimInfo: DimTableInfo,
                         rightExpressions: Seq[Expression],
                         rightDimInfo: DimTableInfo) extends JoinNode

case class LeftJoinNode(left: JoinNode,
                        leftExpressions: Seq[Expression],
                        rightExpressions: Seq[Expression],
                        rightDimInfo: DimTableInfo) extends JoinNode

case class RightJoinNode(right: JoinNode,
                         leftExpressions: Seq[Expression],
                         leftDimInfo: DimTableInfo,
                         rightExpressions: Seq[Expression]) extends JoinNode

case class BushyJoinNode(left: JoinNode,
                         right: JoinNode,
                         leftExpressions: Seq[Expression],
                         rightExpressions: Seq[Expression]) extends JoinNode

object JoinNode {

  def unapply(lp: LogicalPlan): Option[JoinNode] = lp match {
    case ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    PhysicalOperation(leftProjectList, leftFilters, LogicalRelation(leftRelation)),
    PhysicalOperation(rightProjectList, rightFilters, LogicalRelation(rightRelation))) =>
      Some(LeafJoinNode(leftExpressions,
        DimTableInfo(leftProjectList, leftFilters, leftRelation, leftExpressions),
        rightExpressions,
        DimTableInfo(rightProjectList, rightFilters, rightRelation, rightExpressions)))

    case ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    JoinNode(lJT),
    PhysicalOperation(rightProjectList, rightFilters, LogicalRelation(rightRelation))) =>
      Some(LeftJoinNode(lJT, leftExpressions,
        rightExpressions,
        DimTableInfo(rightProjectList, rightFilters, rightRelation, rightExpressions)))

    case ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    PhysicalOperation(leftProjectList, leftFilters, LogicalRelation(leftRelation)),
    JoinNode(rJT)
    ) => Some(RightJoinNode(rJT,
      leftExpressions, DimTableInfo(leftProjectList, leftFilters, leftRelation, leftExpressions),
      rightExpressions))

    case ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    JoinNode(lJT),
    JoinNode(rJT)
    ) => Some(BushyJoinNode(lJT, rJT, leftExpressions, rightExpressions))

    case Project(_, JoinNode(jt)) => Some(jt)

    case _ => None
  }
}

/**
 * A translatable JoinTree must have one of the following forms:
 * - FactTable join DimTable
 * - DimTable join FactTable
 * - FactTable join JoinTree(of Dim table joins)
 * - JoinTree join FactTable
 * - joinPlan join DimTable
 * - DimTable join joinPlan
 *
 * The last 2 forms provide the recursion that enables any kind of JoinTree: lefty, righty or
 * bushy.
 *
 * A Join is translatable if:
 * - it is a [[StarSchema.isStarJoin valid star join]]
 * - the Dimension Table projection and filters are translatable.
 * - for JoinTrees, it must validate for the Facttable's StarSchema
 */
trait JoinTransform {
  self: DruidPlanner =>

  private object JoinDruidQuery {
    def unapply(lp: LogicalPlan): Option[DruidQueryBuilder] = {
      joinPlan(null, lp).headOption
    }
  }

  private def validateJoinCondition(leftExpressions: Seq[Expression],
                                    rightExpressions: Seq[Expression],
                                    dqb: DruidQueryBuilder):
  (String, String, Option[DruidQueryBuilder]) = {
    val (leftTable, righTable) = dqb.drInfo.starSchema.isStarJoin(
      leftExpressions, rightExpressions).getOrElse((null, null))

    leftTable match {
      case null => (leftTable, righTable, None)
      case _ => (leftTable, righTable, Some(dqb))
    }
  }

  private def translateJoin(leftExpressions: Seq[Expression],
                            rightExpressions: Seq[Expression],
                            dqb: DruidQueryBuilder,
                            joinTree: JoinNode
                             ): Seq[DruidQueryBuilder] = {

    implicit val sSchema = dqb.drInfo.starSchema

    val (leftTable, righTable, dqb1) =
      validateJoinCondition(leftExpressions, rightExpressions, dqb)

    dqb1.flatMap { dqb =>
      joinTree.validate match {
        case Right(tMap) if tMap.contains(righTable) => {
          (dqb1 /: tMap.toList) {
        case (dqb2, (tNm, dmInfo)) =>
        translateProjectFilter(dqb2,
        dmInfo.projectList,
        dmInfo.filters,
        true,
        dmInfo.joinAttrs).headOption
        }
        }
        case Right(tMap) => {
          logInfo(s"'$righTable' being joined with JoinTree is not in JoinTree: $joinTree")
          None
        }
        case Left(err) => {
          logInfo(s"Invalid joinTree: $err ($joinTree)")
          None
        }
      }
    }.toSeq
  }

  private def translateJoin(leftExpressions: Seq[Expression],
                            rightExpressions: Seq[Expression],
                            dqb: DruidQueryBuilder,
                            dimProjectList: Seq[NamedExpression],
                            dimFilters: Seq[Expression],
                            dimRelation: BaseRelation
                             ): Seq[DruidQueryBuilder] = {

    val (leftTable, righTable, dqb1) =
      validateJoinCondition(leftExpressions, rightExpressions, dqb)

    val rightJoinAttrs: Set[String] = rightExpressions.map {
      case AttributeReference(nm, _, _, _) => nm
    }.toSet

    translateProjectFilter(dqb1,
      dimProjectList,
      dimFilters,
      false,
      rightJoinAttrs)
  }

  val joinTransform: DruidTransform = {
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    JoinDruidQuery(jdqb),
    PhysicalOperation(projectList, filters, l@LogicalRelation(dimRelation))
    )
      ) => {
      translateJoin(leftExpressions,
        rightExpressions,
        jdqb,
        projectList,
        filters,
        dimRelation
      )
    }
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    PhysicalOperation(projectList, filters, l@LogicalRelation(dimRelation)),
    JoinDruidQuery(jdqb)
    )
      ) => {
      translateJoin(rightExpressions,
        leftExpressions,
        jdqb,
        projectList,
        filters,
        dimRelation
      )
    }
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    JoinDruidQuery(jdqb),
    JoinNode(jN)
    )
      ) => {
      translateJoin(leftExpressions,
        rightExpressions,
        jdqb,
        jN
      )
    }
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    None,
    JoinNode(jN),
    JoinDruidQuery(jdqb)
    )
      ) => {
      translateJoin(rightExpressions,
        leftExpressions,
        jdqb,
        jN
      )
    }
    case (dqb, Project(joinProjectList,
    JoinDruidQuery(jdqb)
    )) => {
      var dqbO: Option[DruidQueryBuilder] = Some(jdqb)
      val joinAttrs = Set[String]()
      dqbO = joinProjectList.foldLeft(dqbO) { (dqB, e) =>
        dqB.flatMap(projectExpression(_, e, joinAttrs))
      }
      dqbO.toSeq
    }
    case _ => Seq()
  }

}

