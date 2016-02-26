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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, Filter, Project, LogicalPlan}
import org.apache.spark.sql.columnar.InMemoryRelation

import org.apache.spark.sql.catalyst.planning.PhysicalOperation.{ReturnType, substitute, collectAliases}
import org.apache.spark.sql.sources.druid.DruidPlanner

/**
  * A thin wrapper on [[org.apache.spark.sql.catalyst.planning.PhysicalOperation]], that
  * replaces an [[org.apache.spark.sql.columnar.InMemoryRelation]] with its original
  * [[LogicalPlan]]
  *
  * @param sqlContext
  */
class CachedTablePattern(val sqlContext : SQLContext)  extends PredicateHelper {

  def cacheManager = sqlContext.cacheManager

  def tablesToCheck : Array[String] = {
    val l = sqlContext.getConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK)
    if (l.isEmpty)  sqlContext.tableNames() else l.toArray
  }

  def logicalPlan(inMemPlan: InMemoryRelation): Option[LogicalPlan] = {

    (
      for (t <- sqlContext.tableNames() if cacheManager.isCached(t);
           ce <- cacheManager.lookupCachedData(sqlContext.table(t))
           if (ce.cachedRepresentation.child == inMemPlan.child)
      ) yield ce.plan
      ).headOption
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = _collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  private type CollectProjectFilterType =
  (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression])

  private def collectInMemoryRelation(iP :InMemoryRelation) : Option[CollectProjectFilterType] =
    logicalPlan(iP).map {
      case Subquery(name, child) => child
     }.map { lP =>
      (
        None.asInstanceOf[Option[Seq[NamedExpression]]],
        Nil.asInstanceOf[Seq[Expression]],
        lP,
        Map.empty.asInstanceOf[Map[Attribute, Expression]]
        )
    }


  private def _collectProjectsAndFilters(plan: LogicalPlan): CollectProjectFilterType =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases) = _collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) =>
        val (fields, filters, other, aliases) = _collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case iP :InMemoryRelation if true  =>
        val r = collectInMemoryRelation(iP)
        if(r.isDefined) {
          r.get
        } else {
          (None, Nil, iP, Map.empty)
        }
      case other =>
        (None, Nil, other, Map.empty)
    }
}
