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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.sparklinedata.druid._
import scala.language.implicitConversions

trait LimitTransfom {
  self: DruidPlanner =>

  /**
   * ==Sort Rewrite:==
   * A '''Sort''' Operator is pushed down to ''Druid'' if all its __order expressions__
   * can be pushed down. An __order expression__ is pushed down if it is on an ''Expression''
   * that is already pushed to Druid, or if it is an [[Alias]] expression whose child
   * has been pushed to Druid.
   *
   * ==Limit Rewrite:==
   * A '''Limit''' Operator above a Sort is always pushed down to Druid. The __limit__
   * value is set on the [[LimitSpec]] of the [[GroupByQuerySpec]]
   */
  val limitTransform: DruidTransform = {
    case (dqb, sort@Sort(orderExprs, global, child: Aggregate)) => {
      // TODO: handle Having
      val dqbs = plan(dqb, child).map { dqb =>
        val exprToDruidOutput =
          new DruidOperatorSchema(dqb).pushedDownExprToDruidAttr

        val dqb1: ODB = orderExprs.foldLeft(Some(dqb).asInstanceOf[ODB]) { (dqb, e) =>
          for (dqb2 <- dqb;
               ue <- unalias(e.child, child);
               doA <- exprToDruidOutput.get(ue))
            yield dqb2.orderBy(doA.name, e.direction == Ascending)
        }
        dqb1
      }
      Utils.sequence(dqbs.toList).getOrElse(Seq())
    }
    case (dqb, sort@Limit(limitExpr, child: Sort)) => {
      val dqbs = plan(dqb, child).map { dqb =>
        val amt = limitExpr.eval(null).asInstanceOf[Int]
        dqb.limit(amt)
      }
      Utils.sequence(dqbs.toList).getOrElse(Seq())
    }
    case _ => Seq()
  }
}

abstract class DruidTransforms extends DruidPlannerHelper
with ProjectFilterTransfom with AggregateTransform with JoinTransform with LimitTransfom
with PredicateHelper with Logging  {
  self: DruidPlanner =>

  type DruidTransform = Function[(Seq[DruidQueryBuilder], LogicalPlan), Seq[DruidQueryBuilder]]
  type ODB = Option[DruidQueryBuilder]

  case class ORTransform(t1 : DruidTransform, t2 : DruidTransform) extends DruidTransform {

    def apply(t : (Seq[DruidQueryBuilder], LogicalPlan)) : Seq[DruidQueryBuilder] = {

      val r = t1((t._1, t._2))
      if (r.size > 0) {
        r
      } else {
        t2((t._1, t._2))
      }
    }
  }

  case class DebugTransform(transformName : String,
                            t : DruidTransform) extends DruidTransform {

    def apply(i : (Seq[DruidQueryBuilder], LogicalPlan)) : Seq[DruidQueryBuilder] = {
      val iDQBs : Seq[DruidQueryBuilder] = i._1
      val lP : LogicalPlan = i._2
      val oDQBs = t(iDQBs, lP)
      if (sqlContext.getConf(DruidPlanner.DEBUG_TRANSFORMATIONS)) {
        log.info(s"$transformName transform invoked:\n" +
          s"Input DruidQueryBuilders : $iDQBs \n" +
          s"Input LogicalPlan : $lP" +
          s"Output DruidQueryBuilders : $oDQBs\n")
      }
      oDQBs
    }
  }

  case class TransformHolder(t : DruidTransform) {

    def or(t2 : DruidTransform) = ORTransform(t, t2)

    def debug(name : String) : DruidTransform = new DebugTransform(name, t)
  }

  implicit def transformToHolder(t : DruidTransform) = TransformHolder(t)

  def debugTranslation(msg : => String) : Unit = {
    if (sqlContext.getConf(DruidPlanner.DEBUG_TRANSFORMATIONS)) {
      logInfo(msg)
    }
  }

}
