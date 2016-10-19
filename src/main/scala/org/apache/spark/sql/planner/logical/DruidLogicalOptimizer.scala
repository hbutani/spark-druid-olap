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
package org.apache.spark.sql.planner.logical

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sparklinedata.shim.SparkShim
import org.apache.spark.sql.util.PlanUtil.maxCardinalityIsOne
import org.apache.spark.sql.util.{ExprUtil, PlanUtil}

object DruidLogicalOptimizer {

  val batches: Seq[(String, SparkShim.RuleStrategy, Rule[LogicalPlan])] = Seq(
    ("Rewrite Sum(Literal) as Count(1)*Literal", SparkShim.fixedPoint(100), SumOfLiteralRewrite),
    ("Push GB through Project, Join", SparkShim.fixedPoint(100), PushGB),
    ("Pull true VC up in to Agg", SparkShim.fixedPoint(100), PullVColsIntoAgg)
  )

  def apply(conf : SQLConf) : Optimizer = {
    SparkShim.extendedlogicalOptimizer(conf, batches)
  }
}

/**
  * {{{
  * GB([Proj](Join([Proj]))) => Proj(Join(GB))
  * PushGB
  *   Push
  *     1.Join is Cross Product)
  *       1.1. Get Push Info
  *         1.1.1. Get Push Candidate Info
  *           1.1.1.1 Join has one side that has aggregate without
  *                   any cardinality augmenters above (Non push side)
  *           1.1.1.2 Join's other side has no agg or there is
  *                   a cardinality augmenter above it (Push Side)
  *         1.1.2. Max cardinality of Non Push Side is 1 and
  *                all of GB/AggFn keys are deterministic
  *         1.1.3. All of agg exprs come from push side
  *         1.1.4. None of GB exprs include attributes from both sides &
  *                there exists GB exprs from push side
  *         1.1.5. If pushside child is project then Translate AggFns, GB Keys below project
  *
  *     2. Setup pipeline
  * }}}
  */
object PushGB extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Push(plan) => plan
  }

  object Push {
    def unapply(op: LogicalPlan): Option[LogicalPlan] = {
      val lpo: Option[(Option[Filter], Aggregate, Option[Project], Join)] = op match {
        case a@Aggregate(ge, ae, p@Project(plst, j@Join(l, r, jt, jc))) if jc.isEmpty =>
          Some(None, a, Some(p), j)
        case a@Aggregate(ge, ae, j@Join(l, r, jt, jc)) if jc.isEmpty => Some(None, a, None, j)
        case _ => None
      }
      for (lp <- lpo; pi <- getPushInfo(lp._2, lp._3, lp._4);
           newLP <- setupNewOpPipeLine(lp._4, pi)) yield
        newLP
    }
  }

  private[this] case class PushInfo(lGBKeys: Seq[Expression], lAggKeys: Seq[NamedExpression],
                                    lFil: Option[Expression],
                                    lChild: LogicalPlan,
                                    rGBKeys: Seq[Expression], rAggKeys: Seq[NamedExpression],
                                    rFil: Option[Expression],
                                    rChild: LogicalPlan,
                                    constGBAggKeys: Seq[NamedExpression],
                                    jt: JoinType,
                                    rqdCols: Seq[String])

  private[this] object PushInfo {
    // Cross Product - pushing to only one side
    def apply(pc: PushCandiates, gbKeys: Seq[Expression], aggKeys: Seq[NamedExpression],
              constGBAggKeys: Seq[NamedExpression], c: LogicalPlan,
              j: Join, oa: Aggregate): PushInfo = {
      val rqdCols = oa.aggregateExpressions.map(ne => ne.name)
      if (pc.pushToL) {
        this (gbKeys, aggKeys, None, c, Seq.empty[Expression],
          Seq.empty[NamedExpression], None, j.right, constGBAggKeys, Inner, rqdCols)
      } else {
        this (Seq.empty[Expression], Seq.empty[NamedExpression],
          None, j.left, gbKeys, aggKeys, None, c, constGBAggKeys, Inner, rqdCols)
      }
    }
  }

  // TODO: Cache the pushInfo for each node so that subsequent calls
  // , due to recursive transforms on the tree, can reuse them
  private[this] def getPushInfo(a: Aggregate, p: Option[Project], j: Join): Option[PushInfo] = {
    var pi: Option[PushInfo] = None

    // Handle Cross Product
    if (j.condition.isEmpty) {
      val pc = getPushCandidates(a, p, j)

      // 1. Can push agg below Join ?
      //    All of the AggFunc, GB Exprs, Join cond is deterministic ?
      //    Non push candidate has cardinality of one ?
      if (pc.nonEmpty &&
        !(pc.get.aggs.toSet ++ pc.get.gbExps.toSet).exists(ag => !ag.deterministic) &&
        ((pc.get.pushToR & maxCardinalityIsOne(j.left)) ||
          (pc.get.pushToL & maxCardinalityIsOne(j.right)))) {
        val pushSideChild = if (pc.get.pushToL) j.left else j.right
        val c1SideChild = if (pc.get.pushToL) j.right else j.left

        // 2. All of Aggr Exprs are from the push candidate side of Join
        val pa = j.references.filter(pushSideChild.outputSet.contains)
        val c1a = j.references.filter(c1SideChild.outputSet.contains)
        val agFSplit = ExprUtil.splitExprs(pc.get.aggs,
          pushSideChild.outputSet, c1SideChild.outputSet)
        if (pc.get.aggs.isEmpty ||
          (agFSplit._1.nonEmpty &&
            !agFSplit._2.exists { e => e match {
              case a@AggregateExpression(_, _, _) => true
              case al@Alias(a@AggregateExpression(_, _, _), _) =>
                if (c1SideChild.outputSet.contains(a.asInstanceOf[NamedExpression])) false else true
              case _ => false
            }
            } &&
            agFSplit._3.isEmpty)) {

          // 3. None of the GB exprs include references to both sides and
          // there exists some GB exprs from push side
          val gbESplit = ExprUtil.splitExprs(pc.get.gbExps,
            pushSideChild.outputSet, c1SideChild.outputSet)
          if (pc.get.gbExps.nonEmpty && gbESplit._1.nonEmpty && gbESplit._3.isEmpty) {

            // 4. Translate GB, Agg keys below Project if
            // pushside child is Project J(GB(P)) => J(p(GB))

            /*
            // Disabled for time being. Seems like there is an issue with
            // AggregateTransform without child project.
            if (pushSideChild.isInstanceOf[Project]) {
              val exprsBelowProj = ExprUtil.translateAggBelowProject(gbESplit._1 ++ gbESplit._4,
                (agFSplit._1 ++ agFSplit._4).asInstanceOf[Seq[NamedExpression]],
                None, pushSideChild.asInstanceOf[Project])
              if (exprsBelowProj.nonEmpty) {
                pi = Some(PushInfo(pc.get.asInstanceOf[PushCandiates], exprsBelowProj.get._1,
                  exprsBelowProj.get._2, agFSplit._2.asInstanceOf[Seq[NamedExpression]],
                  pushSideChild.asInstanceOf[Project].child, j, a))
              }
            } else */{
              pi = Some(PushInfo(pc.get.asInstanceOf[PushCandiates], gbESplit._1 ++ gbESplit._4,
                (agFSplit._1 ++ agFSplit._4).asInstanceOf[Seq[NamedExpression]],
                agFSplit._2.asInstanceOf[Seq[NamedExpression]], pushSideChild, j, a))
            }
          }
        }
      }
    }
    pi
  }

  case class PushCandiates(pushToL: Boolean, pushToR: Boolean,
                           aggs: Seq[NamedExpression], gbExps: Seq[Expression])

  // TODO: 1. extend for Join Condition LOJ, ROJ, FOJ, SJ
  //       2. Allow Agg to be pushed to a side even
  //         if that branch already has one (based on cost)
  /**
    * Identify Join child that may be a candidate for Aggregate push.
    * if GB child is Projectm then translate GB, Agg keys below project.
    * Currenlty we only push if join is crossproduct & if aggregate
    * is not present on a side
    *
    * @param j       Join
    * @param gbKeys  GB Keys
    * @param aggKeys Aggregate Function Keys
    * @return
    */
  private[this] def getPushCandidates(a: Aggregate, p: Option[Project], j: Join,
                                      gbKeys: Seq[Expression] = Nil,
                                      aggKeys: Seq[NamedExpression] = Nil):
  Option[PushCandiates] = {
    var pc: Option[PushCandiates] = None
    if ((j.joinType == Inner) && j.condition.isEmpty &&
      (a.aggregateExpressions.nonEmpty || a.groupingExpressions.nonEmpty)) {
      val pushToL: Boolean = isPushCandidate(j.left)
      val pushToR: Boolean = isPushCandidate(j.right)

      if ((pushToL || pushToR) && !(pushToL && pushToR)) {
        val te: Option[(Seq[Expression], Seq[NamedExpression], Option[Expression], LogicalPlan)] =
          if (p.nonEmpty) {
            ExprUtil.translateAggBelowProject(
              a.groupingExpressions, a.aggregateExpressions, None, p.get)
          } else {
            Some(a.groupingExpressions, a.aggregateExpressions, None, j)
          }
        if (te.nonEmpty) {
          pc = Some(PushCandiates(pushToL, pushToR, te.get._2, te.get._1))
        }
      }
    }
    pc
  }

  private[this] def isPushCandidate(lp: LogicalPlan): Boolean = (!lp.isInstanceOf[Aggregate]) && {
    val aggs = lp.collect { case ag: Aggregate => ag }
    (aggs.isEmpty || PlanUtil.isCardinalityAugmented(lp, aggs.asInstanceOf[Seq[LogicalPlan]]))
  }

  private[this] def setupNewOpPipeLine(j: Join, pi: PushInfo): Option[LogicalPlan] = {
    val newLChild = if (pi.lGBKeys.nonEmpty || pi.lAggKeys.nonEmpty) {
      Aggregate(pi.lGBKeys, pi.lAggKeys, pi.lChild)
    } else {
      j.left
    }
    val newRChild = if (pi.rGBKeys.nonEmpty || pi.rAggKeys.nonEmpty) {
      Aggregate(pi.rGBKeys, pi.rAggKeys, pi.rChild)
    } else {
      j.right
    }
    val newJoin = Join(newLChild.asInstanceOf[LogicalPlan],
      newRChild.asInstanceOf[LogicalPlan], pi.jt, None)
    val newAggKeys = (newJoin.output.toSeq ++ pi.constGBAggKeys)
    val plst = pi.rqdCols.flatMap(cn =>
      newAggKeys.collectFirst { case ne: NamedExpression if cn.equals(ne.name) => ne })
    if (plst.size == pi.rqdCols.size) {
      Some(Project(plst, newJoin))
    } else {
      None
    }
  }
}

object SumOfLiteralRewrite extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case RewriteSum(plan) => plan
  }

  object RewriteSum {
    def unapply(op: LogicalPlan): Option[LogicalPlan] = op match {
      case p@Project(se, Aggregate(ge, ae, c)) if rewriteCandidate(ae) =>
        for (sli <- sumLiteralInfo(ae)) yield {
          val newAgg = Aggregate(ge, sli._1, c)
          val sMap = newAgg.aggregateExpressions.foldLeft(Map[String, Expression]()) { (m, e) =>
            if (sli._2.contains(e.name)) {
              m + (e.name -> Multiply(AttributeReference(e.name, e.dataType, e.nullable,
                e.metadata)(e.exprId, e.qualifiers), sli._2.get(e.name).get._1))
            } else {
              m
            }
          }
          val pl = se.map(e => ExprUtil.translateExpr(e, sMap)).asInstanceOf[Seq[NamedExpression]]
          Project(pl, newAgg)
        }
      case a@Aggregate(ge, ae, c) if rewriteCandidate(ae) =>
        for (sli <- sumLiteralInfo(ae)) yield {
          val newAgg = Aggregate(ge, sli._1, c)
          val pl: Seq[NamedExpression] = newAgg.aggregateExpressions.map {
            case ne: NamedExpression if (sli._2.contains(ne.name)) =>
              val ale = sli._2.get(ne.name).get
              Alias(Multiply(AttributeReference(ne.name, ne.dataType, ne.nullable, ne.metadata)
              (ne.exprId, ne.qualifiers), ale._1), ne.name)(ale._2.exprId,
                ale._2.qualifiers, ale._2.explicitMetadata)
            case ne: NamedExpression => AttributeReference(ne.name, ne.dataType, ne.nullable,
              ne.metadata)(ne.exprId, ne.qualifiers)
          }
          Project(pl.asInstanceOf[Seq[NamedExpression]], newAgg)
        }
      case _ => None
    }

    private[this] def rewriteCandidate(ae: Seq[NamedExpression]) = ae.exists {
      case Alias(AggregateExpression(Sum(Literal(v, _)), _, _), _) if (v != null) => true
      case _ => false
    }

    private[this] def sumLiteralInfo(ae: Seq[NamedExpression]):
    Option[(Seq[NamedExpression], Map[String, (Literal, Alias)])] = {
      val sumLitInf =
        ae.foldLeft((Seq[NamedExpression](), Map[String, (Literal, Alias)]()))((t, e) => e match {
          case al@Alias(ae@AggregateExpression(Sum(l@Literal(lv, _)), _, _), n) if (lv != null) =>
            ((t._1 :+
              new Alias(AggregateExpression(Count(Literal(1)), ae.mode, ae.isDistinct), n)()),
              t._2 + (n ->(l, al)))
          case ag@_ => ((t._1 :+ ag), t._2)
        })
      if (sumLitInf._2.isEmpty) None else Some(sumLitInf)
    }
  }

}

object PullVColsIntoAgg extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Pull(plan) => plan
  }

  object Pull {
    def unapply(op: LogicalPlan): Option[LogicalPlan] = op match {
      case a@Aggregate(ge, ae, p@Project(plst, pc)) if plst.forall(pe => pe.deterministic) &&
          plst.exists(pe =>
            pe match{
            case Alias(ae, _) if ae.children.size > 1 => true
            case _ => false}) =>
        for (te <- ExprUtil.translateAggBelowProject(ge, ae, None, p)) yield {
          val newChildOp = te._4 match {
            case Project(_, _) => te._4
            case _ => Project(
              (te._1 ++ te._2).foldLeft(Seq[Attribute]())((ars, e) => ars ++ e.references.toSeq),
              te._4)
          }
          Aggregate(te._1, te._2, newChildOp)
        }
      case _ => None
    }
  }

}