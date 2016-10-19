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

import org.apache.spark.sql.CachedTablePattern
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, AttributeReference, NamedExpression, Expression}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ExtractEquiJoinKeys}
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Project, LogicalPlan}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
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
  val otherJoinPredicate : Option[Expression]

  private def checkStarJoin(tables: Map[String, DimTableInfo])
                           (implicit sSchema: StarSchema
                           ): Either[String, Map[String, DimTableInfo]] = {
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
    *
    * @param sSchema
   * @return
   */
  def validate(implicit sSchema: StarSchema):
  Either[String, Map[String, DimTableInfo]] = this match {
    case n: LeafJoinNode => checkStarJoin(Map())
    case LeftJoinNode(left, lE, rE, _, _) => checkOnesidedJoinTree(left)
    case RightJoinNode(right, lE, rE, _, _) => checkOnesidedJoinTree(right)
    case BushyJoinNode(left, right, _, _, _) => {
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
                         rightDimInfo: DimTableInfo,
                         otherJoinPredicate : Option[Expression]) extends JoinNode

case class LeftJoinNode(left: JoinNode,
                        leftExpressions: Seq[Expression],
                        rightExpressions: Seq[Expression],
                        rightDimInfo: DimTableInfo,
                        otherJoinPredicate : Option[Expression]) extends JoinNode

case class RightJoinNode(right: JoinNode,
                         leftExpressions: Seq[Expression],
                         leftDimInfo: DimTableInfo,
                         rightExpressions: Seq[Expression],
                         otherJoinPredicate : Option[Expression]) extends JoinNode

case class BushyJoinNode(left: JoinNode,
                         right: JoinNode,
                         leftExpressions: Seq[Expression],
                         rightExpressions: Seq[Expression],
                         otherJoinPredicate : Option[Expression]) extends JoinNode

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
  self: DruidPlanner with PredicateHelper =>

  private object JoinQueryMatch {
    def unapply(arg : (LogicalPlan, Boolean)): Option[DruidQueryBuilder] = {
      val lp = arg._1
      val mustHaveJoin = arg._2
      val t = if (mustHaveJoin) joinTransformWithDebug else joinGraphTransform
      t(null, lp).headOption
    }
  }

  private object JoinGraphDruidQuery {
    def unapply(lp: LogicalPlan): Option[DruidQueryBuilder] = (lp, false) match {
      case JoinQueryMatch(jdqb) => Some(jdqb)
      case _ => None
    }
  }

  private object JoinDruidQuery {
    def unapply(lp: LogicalPlan): Option[DruidQueryBuilder] = (lp, true) match {
      case JoinQueryMatch(jdqb) => Some(jdqb)
      case _ => None
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

  private def translateOtherJoinPredicates(dqb : Option[DruidQueryBuilder],
                                           otherJoinPredicate : Option[Expression])
  : Seq[DruidQueryBuilder] = (dqb, otherJoinPredicate) match {
    case(None, _) => Seq()
    case (Some(dqb), None) => Seq(dqb)
    case (Some(dqb), Some(p)) => translateProjectFilter(Some(dqb),
      Seq(),
      splitConjunctivePredicates(p),
      true,
      Set())
  }

  private def translateOtherJoinPredicates(dqb : Option[DruidQueryBuilder],
                                           joinTree : JoinNode)
  : Seq[DruidQueryBuilder] = (dqb, joinTree) match {
    case(None, _) => Seq()
    case (Some(dqb), LeafJoinNode(_,_,_,_,None)) => Seq(dqb)
    case (Some(dqb), LeafJoinNode(_,_,_,_,Some(p))) => translateProjectFilter(Some(dqb),
      Seq(),
      splitConjunctivePredicates(p),
      true,
      Set())
    case (Some(dqb), LeftJoinNode(lTree,_,_,_,p)) => {
      val dqb2 = translateOtherJoinPredicates(Some(dqb), lTree)
      p match {
        case None => dqb2
        case Some(p) => translateOtherJoinPredicates(dqb2.headOption, Some(p))
      }
    }
    case (Some(dqb), RightJoinNode(lTree,_,_,_,p)) => {
      val dqb2 = translateOtherJoinPredicates(Some(dqb), lTree)
      p match {
        case None => dqb2
        case Some(p) => translateOtherJoinPredicates(dqb2.headOption, Some(p))
      }
    }
    case (Some(dqb), BushyJoinNode(lTree,rTree,_,_,p)) => {
      val dqb2 = translateOtherJoinPredicates(Some(dqb), lTree).flatMap(dqb4 =>
        translateOtherJoinPredicates(Some(dqb4), rTree)
      )
      p match {
        case None => dqb2
        case Some(p) => translateOtherJoinPredicates(dqb2.headOption, Some(p))
      }
    }

  }

  private def translateJoin(leftExpressions: Seq[Expression],
                            rightExpressions: Seq[Expression],
                           otherJoinPredicate : Option[Expression],
                            dqb: DruidQueryBuilder,
                            joinTree: JoinNode
                             ): Seq[DruidQueryBuilder] = {

    implicit val sSchema = dqb.drInfo.starSchema

    val (leftTable, righTable, dqb1) =
      validateJoinCondition(leftExpressions, rightExpressions, dqb)

    val dqb2 = dqb1.flatMap { dqb =>
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
    }

    translateOtherJoinPredicates(
      translateOtherJoinPredicates(dqb2, joinTree).headOption,
      otherJoinPredicate
    )

  }

  private def translateJoin(leftExpressions: Seq[Expression],
                            rightExpressions: Seq[Expression],
                            otherJoinPredicate : Option[Expression],
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

    val dqb2 = translateProjectFilter(dqb1,
      dimProjectList,
      dimFilters,
      true,
      rightJoinAttrs).headOption

    translateOtherJoinPredicates(dqb2, otherJoinPredicate)
  }

  val joinTransform: DruidTransform = {
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    otherJoinPredicate,
    JoinGraphDruidQuery(jdqb),
    cacheTablePatternMatch(projectList, filters, l@LogicalRelation(dimRelation, _, _))
    )
      ) => {
      translateJoin(leftExpressions,
        rightExpressions,
        otherJoinPredicate,
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
    otherJoinPredicate,
    cacheTablePatternMatch(projectList, filters, l@LogicalRelation(dimRelation, _,_)),
    JoinGraphDruidQuery(jdqb)
    )
      ) => {
      translateJoin(rightExpressions,
        leftExpressions,
        otherJoinPredicate,
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
    otherJoinPredicate,
    JoinGraphDruidQuery(jdqb),
    JoinNode(jN)
    )
      ) => {
      translateJoin(leftExpressions,
        rightExpressions,
        otherJoinPredicate,
        jdqb,
        jN
      )
    }
    case (dqb, ExtractEquiJoinKeys(
    Inner,
    leftExpressions,
    rightExpressions,
    otherJoinPredicate,
    JoinNode(jN),
    JoinGraphDruidQuery(jdqb)
    )
      ) => {
      translateJoin(rightExpressions,
        leftExpressions,
        otherJoinPredicate,
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
        dqB.flatMap(projectExpression(_, e, joinAttrs, true))
      }
      dqbO.toSeq
    }
    case _ => Seq()
  }

  object JoinNode {

    def unapply(lp: LogicalPlan): Option[JoinNode] = lp match {
      case ExtractEquiJoinKeys(
      Inner,
      leftExpressions,
      rightExpressions,
      otherJoinPredicate,
      cacheTablePatternMatch(leftProjectList, leftFilters, LogicalRelation(leftRelation, _, _)),
      cacheTablePatternMatch(
      rightProjectList, rightFilters, LogicalRelation(rightRelation, _, _))) =>
        Some(LeafJoinNode(leftExpressions,
          DimTableInfo(leftProjectList, leftFilters, leftRelation, leftExpressions),
          rightExpressions,
          DimTableInfo(rightProjectList, rightFilters, rightRelation, rightExpressions),
          otherJoinPredicate))

      case ExtractEquiJoinKeys(
      Inner,
      leftExpressions,
      rightExpressions,
      otherJoinPredicate,
      JoinNode(lJT),
      cacheTablePatternMatch(
      rightProjectList, rightFilters, LogicalRelation(rightRelation, _, _))) =>
        Some(LeftJoinNode(lJT, leftExpressions,
          rightExpressions,
          DimTableInfo(rightProjectList, rightFilters, rightRelation, rightExpressions),
          otherJoinPredicate))

      case ExtractEquiJoinKeys(
      Inner,
      leftExpressions,
      rightExpressions,
      otherJoinPredicate,
      cacheTablePatternMatch(leftProjectList, leftFilters, LogicalRelation(leftRelation, _, _)),
      JoinNode(rJT)
      ) => Some(RightJoinNode(rJT,
        leftExpressions, DimTableInfo(leftProjectList, leftFilters, leftRelation, leftExpressions),
        rightExpressions,
        otherJoinPredicate))

      case ExtractEquiJoinKeys(
      Inner,
      leftExpressions,
      rightExpressions,
      otherJoinPredicate,
      JoinNode(lJT),
      JoinNode(rJT)
      ) => Some(BushyJoinNode(lJT, rJT, leftExpressions, rightExpressions, otherJoinPredicate))

      case Project(_, JoinNode(jt)) => Some(jt)

      case _ => None
    }
  }

}

