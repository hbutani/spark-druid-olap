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

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, Filter, Project, LogicalPlan}
import org.apache.spark.sql.execution.columnar.InMemoryRelation

import org.apache.spark.sql.catalyst.planning.PhysicalOperation.{ReturnType}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources.druid.DruidPlanner

import scala.collection.mutable.{Map => mMap}

/**
  * A thin wrapper on [[org.apache.spark.sql.catalyst.planning.PhysicalOperation]], that
  * replaces an [[org.apache.spark.sql.execution.columnar.InMemoryRelation]] with its original
  * [[LogicalPlan]]
  *
  * @param sqlContext
  */
class CachedTablePattern(val sqlContext : SQLContext)  extends PredicateHelper {

  def cacheManager = sqlContext.cacheManager

  @transient
  private val cacheLock = new ReentrantReadWriteLock

  private var associationCache : mMap[SparkPlan, LogicalPlan] = mMap()

  private def readLock[A](f: => A): A = {
    val lock = cacheLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val lock = cacheLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private def getFromCache(inMemPlan: InMemoryRelation) : Option[LogicalPlan] = readLock {
    associationCache.get(inMemPlan.child)
  }

  private def putInCache(inMemPlan: InMemoryRelation, lP : Option[LogicalPlan]) : Unit = writeLock {
    lP.foreach { lP =>
      associationCache.put(inMemPlan.child, lP)
    }
  }

  def tablesToCheck : Array[String] = {
    val l = sqlContext.getConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK)
    l match {
      case l if l.isEmpty => sqlContext.tableNames()
      case l if l.size == 1 && l(0).trim == "" => Array()
      case _ => l.toArray
    }
  }

  def logicalPlan(inMemPlan: InMemoryRelation): Option[LogicalPlan] = {
      var lP: Option[LogicalPlan] = getFromCache(inMemPlan)

      if (lP.isEmpty) {
        lP = (
          for (t <- tablesToCheck;
               ce <- cacheManager.lookupCachedData(sqlContext.table(t)) if sqlContext.isCached(t);
               if (ce.cachedRepresentation.child == inMemPlan.child)
          ) yield ce.plan
          ).headOption
        putInCache(inMemPlan, lP)
      }

      lP
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
      case x => x
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

  private def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  private def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}
