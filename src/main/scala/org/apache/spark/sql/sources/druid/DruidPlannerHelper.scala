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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.types.DataType
import org.sparklinedata.druid.DruidOperatorAttribute

trait DruidPlannerHelper {

  def unalias(e: Expression, agg: Aggregate): Option[Expression] = {

    agg.aggregateExpressions.find { aE =>
      (aE, e) match {
        case _ if aE == e => true
        case (_, e:AttributeReference) if e.exprId == aE.exprId => true
        case (Alias(child, _), e) if child == e => true
        case _ => false
      }
    }.map {
      case Alias(child, _) => child
      case x => x
    }

  }

  def findAttribute(e: Expression): Option[AttributeReference] = {
    e.find(_.isInstanceOf[AttributeReference]).map(_.asInstanceOf[AttributeReference])
  }

  def positionOfAttribute(e: Expression,
                          plan: LogicalPlan): Option[(Expression, (AttributeReference, Int))] = {
    for (aR <- findAttribute(e);
         attr <- plan.output.zipWithIndex.find(t => t._1.exprId == aR.exprId))
      yield (e, (aR, attr._2))
  }

  def exprIdToAttribute(e: Expression,
                          plan: LogicalPlan): Option[(ExprId, Int)] = {
    for (aR <- findAttribute(e);
         attr <- plan.output.zipWithIndex.find(t => t._1.exprId == aR.exprId))
      yield (aR.exprId, attr._2)
  }

  /**
   *
   * @param gEs
   * @param expandOpGExps if Expand is below this GBy, then the corresponding expression
    *                      for each GE in the Expand projection.
   * @param aEs
   * @param expandOpProjection if Expand is below this GBy, then this Expand projection
   * @param aEExprIdToPos if Expand is below this GBy, the pos in the Expand projection
   * @param aEToLiteralExpr for expressions that represent a 'null' value for
   *                        a this GroupingSet or represent the 'grouping__id'
   *                        columns, this is a map to the Literal value that is
   *                        filled in the Projection above the DruidRDD.
   */
  case class GroupingInfo(gEs: Seq[Expression],
                          expandOpGExps : Seq[Expression],
                          aEs: Seq[NamedExpression],
                          expandOpProjection : Seq[Expression],
                          aEExprIdToPos : Map[ExprId, Int],
                          aEToLiteralExpr: Map[Expression, Expression] = Map())

}
