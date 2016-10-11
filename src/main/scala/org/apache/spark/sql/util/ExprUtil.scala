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

package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.joda.time.DateTime
import org.sparklinedata.druid.DruidQueryBuilder
import org.sparklinedata.druid.metadata.DruidDimension


object ExprUtil {

  /**
    * If any input col/ref is null then expression will evaluate to null
    * and if no input col/ref is null then expression won't evaluate to null
    *
    * @param e Expression that needs to be checked
    * @return
    */
  def nullPreserving(e: Expression): Boolean = e match {
    case Literal(v, _) if v == null => false
    case _ if e.isInstanceOf[LeafExpression] => true
    // TODO: Expand the case below
    case (Cast(_, _) | BinaryArithmetic(_) | UnaryMinus(_) | UnaryPositive(_) | Abs(_))
    => e.children.filter(_.isInstanceOf[Expression]).foldLeft(true) {
      (s, ce) => val cexp = nullPreserving(ce); if (s && cexp) return true else false
    }
    case _ => false
  }

  def conditionalExpr(e: Expression): Boolean = {
    e match {
      case If(_, _, _) | CaseWhen(_) | CaseKeyWhen(_, _) | Least(_) | Greatest(_)
           | Coalesce(_) | NaNvl(_, _) | AtLeastNNonNulls(_, _) => true
      case _ if e.isInstanceOf[LeafExpression] => false
      case _ =>
        e.children.filter(_.isInstanceOf[Expression]).foldLeft(false) {
          (s, ce) => val cexp = conditionalExpr(ce); if (s || cexp) return true else false
        }
    }
  }

  def escapeLikeRegex(v: String): String =
    org.apache.spark.sql.catalyst.util.StringUtils.escapeLikeRegex(v)

  def toDateTime(sparkDateLiteral: Int): DateTime = {
    new DateTime(DateTimeUtils.toJavaDate(sparkDateLiteral))
  }

  def isNumeric(dt: DataType) = NumericType.acceptsType(dt)

  /**
    * This is different from transformDown because if rule transforms an Expression,
    * we don't try to apply any more transformation.
    *
    * @param e    Expression
    * @param rule Rule to apply
    * @return
    */
  def transformReplace(e: Expression,
                       rule: PartialFunction[Expression, Expression]): Expression = {
    val afterRule = CurrentOrigin.withOrigin(e.origin) {
      rule.applyOrElse(e, identity[Expression])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (e fastEquals afterRule) {
      e.transformDown(rule)
    } else {
      afterRule
    }
  }

  /**
    * Simplify Cast expression by removing inner most cast if reduendant
    *
    * @param e   Inner Expression
    * @param edt Outer cast DataType
    * @return
    */
  def simplifyCast(e: Expression, edt: DataType): Expression = e match {
    case Cast(ie, idt) if edt.isInstanceOf[NumericType] && (idt.isInstanceOf[DoubleType] ||
      idt.isInstanceOf[FloatType] || idt.isInstanceOf[DecimalType]) =>
      Cast(ie, edt)
    case _ => e
  }

  /**
    * Simplify given predicates
    *
    * @param dqb  DruidQueryBuilder
    * @param fils Predicates
    * @return
    */
  def simplifyPreds(dqb: DruidQueryBuilder, fils: Seq[Expression]): Seq[Expression] =
    fils.foldLeft[Seq[Expression]](List[Expression]()) { (l, f) =>
      var tpl = l
      for (tp <- simplifyPred(dqb, f))
        tpl = l :+ tp
      tpl
    }

  /**
    * Simplify given Predicate. Does rewrites for cast, Conj/Disj, not null expressions.
    *
    * @param dqb DruidQueryBuilder
    * @param fil Predicate
    * @return
    */
  def simplifyPred(dqb: DruidQueryBuilder, fil: Expression): Option[Expression] = fil match {
    case And(le, re) => simplifyBinaryPred(dqb, le, re, true)
    case Or(le, re) => simplifyBinaryPred(dqb, le, re, false)
    case SimplifyCast(e) => simplifyPred(dqb, e)
    case e@_ => e match {
      case SimplifyNotNullFil(se) =>
        var nullFil: Option[Expression] = Some(se)
        if (se.nullable) {
          if (ExprUtil.nullPreserving(se)) {
            val v1 = nullableAttributes(dqb, se.references)
            if (v1._2.isEmpty) {
              if (v1._1.nonEmpty) {
                nullFil = Some((v1._1).foldLeft[Expression](null)((es, ar) =>
                  if (es != null) {
                    And(es, IsNotNull(ar.asInstanceOf[Expression]))
                  } else {
                    IsNotNull(ar.asInstanceOf[Expression])
                  }))
              } else {
                nullFil = None
              }
            }
          }
        } else {
          nullFil = None
        }
        nullFil
      case _ => Some(e)
    }
  }

  /**
    * Simplify Binary Predicate
    *
    * @param dqb  DruidQueryBuilder
    * @param c1   First child (Left)
    * @param c2   Second child (Right)
    * @param conj If true this is a conjuction else disjunction
    * @return
    */
  def simplifyBinaryPred(dqb: DruidQueryBuilder, c1: Expression, c2: Expression, conj: Boolean):
  Option[Expression] = {
    var newFil: Option[Expression] = None
    val newLe = simplifyPred(dqb, c1)
    val newRe = simplifyPred(dqb, c2)
    if (newLe.nonEmpty) {
      newFil = newLe
    }
    if (newRe.nonEmpty) {
      if (newLe.nonEmpty) {
        if (conj) {
          newFil = Some(And(newLe.get, newRe.get))
        } else {
          newFil = Some(Or(newLe.get, newRe.get))
        }
      } else {
        newFil = newRe
      }
    }
    newFil
  }

  /**
    * Split the given AttributeSet to nullable and unresolvable.
    *
    * @param dqb   DruidQueryBuilder
    * @param attrs AttributeSet
    * @return
    */
  def nullableAttributes(dqb: DruidQueryBuilder, attrs: AttributeSet):
  (List[AttributeReference], List[Attribute]) =
    attrs.foldLeft((List[AttributeReference](), List[Attribute]())) { (s, a) =>
      var s1 = s._1
      var s2 = s._2
      val c = dqb.druidColumn(a.name)
      if (c.nonEmpty) {
        c.get match {
            // TODO: revisit
            // for now treat spatialIndex, hllMetric and sketchMetric
            // as not nullable
          case d if d.isDimension(true) && a.isInstanceOf[AttributeReference] =>
            s1 = s1 :+ a.asInstanceOf[AttributeReference]
          case _ => None
        }
      } else {
        s2 = (s2 :+ a)
      }
      (s1, s2)
    }

  private[this] object SimplifyNotNullFil {
    private[this] val trueFil = Literal(true)

    def unapply(e: Expression): Option[Expression] = e match {
      case Not(IsNull(c)) if (c.nullable) => Some(IsNotNull(c))
      case IsNotNull(c) if (c.nullable) => Some(c)
      case Not(IsNull(c)) if (!c.nullable) => Some(trueFil)
      case IsNotNull(c) if (!c.nullable) => Some(trueFil)
      case _ => None
    }
  }

  private[this] object SimplifyCast {
    def unapply(e: Expression): Option[Expression] = e match {
      case Cast(ie@Cast(_, _), edt) =>
        val nie = simplifyCast(ie, edt)
        if (nie != ie) Some(nie) else None
      case _ => None
    }
  }

  /**
    * Given a list of exprs and two AttributeSet, split exprs into those that
    * involves attributes only from first set, only from second set, from both and
    * that doesn't belong to neither (i.e f(Literals)).
    *
    * @param el Expression List
    * @param fa First Attribute Set
    * @param sa Second Attribute Set
    * @return
    */
  def splitExprs(el: Seq[Expression], fa: AttributeSet, sa: AttributeSet):
  (Seq[Expression], Seq[Expression], Seq[Expression], Seq[Expression]) = {
    val (fEL, rest1) =
      el.partition(_.references subsetOf fa)
    val (sEL, rest2) =
      rest1.partition(_.references subsetOf sa)
    val (bEL, nEL) =
      rest2.partition(_.references subsetOf (fa ++ sa))
    (fEL, sEL, bEL, nEL)
  }

  /**
    * Translate Give Aggregate to the below given child project
    *
    * @param gbKeys  GBKeys
    * @param aggKeys Aggregate Keys
    * @param fil     Optional filter
    * @param p       Child Project
    * @return
    */
  def translateAggBelowProject(gbKeys: Seq[Expression],
                               aggKeys: Seq[NamedExpression],
                               fil: Option[Expression], p: Project):
  Option[(Seq[Expression], Seq[NamedExpression], Option[Expression], LogicalPlan)] = {
    p match {
      case p@Project(_, _) if gbKeys.nonEmpty || aggKeys.nonEmpty =>
        val tGBKeys = ExprUtil.translateExprDown(gbKeys, p, false)
        val tAggKeys = ExprUtil.translateExprDown(aggKeys, p)
        val tFil = if (fil.nonEmpty) ExprUtil.translateExprDown(fil.get, p, true) else None
        if ((gbKeys.isEmpty || tGBKeys.nonEmpty) &&
          (aggKeys.isEmpty || tAggKeys.nonEmpty) && (fil.isEmpty || tFil.nonEmpty)) {
          Some(tGBKeys.get, tAggKeys.get.asInstanceOf[Seq[NamedExpression]], tFil, p.child)
        } else {
          None
        }
      case _ => None
    }
  }

  /**
    * Translate given seq of expressions below the child.
    *
    * @param se Sequence of expressions to be translated
    * @param c  Child node
    * @param preserveAlias Should we preserve alias of expression
    * @return
    */
  def translateExprDown(se: Seq[Expression], c: LogicalPlan, preserveAlias: Boolean = true)
  : Option[Seq[Expression]] = {
    val translatedExprs = se.collect { case e: Expression =>
      val v = ExprUtil.translateExprDown(e, c, preserveAlias)
      if (v.nonEmpty) v.get
    }
    if (translatedExprs.size == se.size) {
      Some(translatedExprs.asInstanceOf[Seq[Expression]])
    } else {
      None
    }
  }


  /**
    * Translate given expression through its child as an expression above grand child.
    * Currently we only translate through Project.
    * TODO: add support for Aggregate, Window, Cube, GroupingSet
    *
    * @param e Expression to translate
    * @param c Child node
    * @param preserveAlias Should we preserve alias of expression
    * @return
    */
  def translateExprDown(e: Expression, c: LogicalPlan, preserveAlias: Boolean)
  : Option[Expression] = {
    if (c.isInstanceOf[Project]) {
      val aliasMap = AttributeMap(c.asInstanceOf[Project].projectList.collect {
        case a: Alias => (a.toAttribute, a.child)
      })
      e match {
        case a@Alias(c, n) if preserveAlias => Some(Alias(c.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        }, n)(a.exprId, a.qualifiers, a.explicitMetadata))
        case atr@AttributeReference(n, dt, nul, m) if preserveAlias => Some(Alias(e.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        }, n)(atr.exprId, atr.qualifiers, Some(atr.metadata)))
        case _ => Some(e.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        })
      }
    } else {
      None
    }
  }


  /**
    * Translate given expression by replacing the aliases
    * with new expr (if they are present in the map)
    *
    * @param e Expression to translate
    * @param aliasToNewExpr Map to aliases to new Expression that replaces it
    * @return
    */
  def translateExpr(e: Expression, aliasToNewExpr: Map[String, Expression])
  : Expression = {
    val ne =
      e match {
        case a@Alias(c, n) => Alias(c.transform {
          case at: Attribute => aliasToNewExpr.get(at.name).getOrElse(at)
        }, n)(a.exprId, a.qualifiers, a.explicitMetadata)
        case atr@AttributeReference(n, dt, nul, m) => Alias(e.transform {
          case at: Attribute => aliasToNewExpr.get(at.name).getOrElse(at)
        }, n)(atr.exprId, atr.qualifiers, Some(atr.metadata))
        case _ => e.transform {
          case at: Attribute => aliasToNewExpr.get(at.name).getOrElse(at)
        }
      }
    ne
  }

  def and(exprs : Seq[Expression]) : Option[Expression] = exprs.size match {
    case 0 => None
    case 1 => exprs.headOption
    case _ => Some(exprs.tail.fold(exprs.head)(And(_,_)))
  }

}