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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.IntegerType

// scalastyle:off line.size.limit
/**
  * Copied from [[org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates|RewriteDistinctAggregates]].
  * This gets applied at the start of the [[org.apache.spark.sql.sources.druid.DruidStrategy\DruidStrategy]]
  * since we don't handle any [[Aggregate]] plans with ''distinct'' functions, we assume these plans were
  * replaced by plans that use the [[Expand]] operator to performa multiple aggregations in 1 dataflow.
  *
  * The only difference in this '''Rule''' is that the transformation is applied if there is
  * atleast 1 ''distinct'' aggregation as opposed being applied when there are more than 1 distinct
  * aggregations.
  */
object SPLRewriteDistinctAggregates extends Rule[LogicalPlan] {

  // scalastyle:on
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case a: Aggregate => rewrite(a)
  }

  def rewrite(a: Aggregate): Aggregate = {

    // Collect all aggregate expressions.
    val aggExpressions = a.aggregateExpressions.flatMap { e =>
      e.collect {
        case ae: AggregateExpression => ae
      }
    }

    // Extract distinct aggregate expressions.
    val distinctAggGroups = aggExpressions
      .filter(_.isDistinct)
      .groupBy(_.aggregateFunction.children.toSet)

    // Check if the aggregates contains functions that do not support partial aggregation.
    val existsNonPartial = aggExpressions.exists(!_.aggregateFunction.supportsPartial)

    // Aggregation strategy can handle queries with a single distinct group and partial aggregates.
    if (distinctAggGroups.size > 0 || (distinctAggGroups.size == 1 && existsNonPartial)) {
      // Create the attributes for the grouping id and the group by clause.
      val gid = AttributeReference("gid", IntegerType, nullable = false)(isGenerated = true)
      val groupByMap = a.groupingExpressions.collect {
        case ne: NamedExpression => ne -> ne.toAttribute
        case e => e -> AttributeReference(e.sql, e.dataType, e.nullable)()
      }
      val groupByAttrs = groupByMap.map(_._2)

      // Functions used to modify aggregate functions and their inputs.
      def evalWithinGroup(id: Literal, e: Expression) = If(EqualTo(gid, id), e, nullify(e))
      def patchAggregateFunctionChildren(
                                          af: AggregateFunction)(
                                          attrs: Expression => Expression): AggregateFunction = {
        af.withNewChildren(af.children.map(attrs)).asInstanceOf[AggregateFunction]
      }

      // Setup unique distinct aggregate children.
      val distinctAggChildren = distinctAggGroups.keySet.flatten.toSeq.distinct
      val distinctAggChildAttrMap = distinctAggChildren.map(expressionAttributePair)
      val distinctAggChildAttrs = distinctAggChildAttrMap.map(_._2)

      // Setup expand & aggregate operators for distinct aggregate expressions.
      val distinctAggChildAttrLookup = distinctAggChildAttrMap.toMap
      val distinctAggOperatorMap = distinctAggGroups.toSeq.zipWithIndex.map {
        case ((group, expressions), i) =>
          val id = Literal(i + 1)

          // Expand projection
          val projection = distinctAggChildren.map {
            case e if group.contains(e) => e
            case e => nullify(e)
          } :+ id

          // Final aggregate
          val operators = expressions.map { e =>
            val af = e.aggregateFunction
            val naf = patchAggregateFunctionChildren(af) { x =>
              evalWithinGroup(id, distinctAggChildAttrLookup(x))
            }
            (e, e.copy(aggregateFunction = naf, isDistinct = false))
          }

          (projection, operators)
      }

      // Setup expand for the 'regular' aggregate expressions.
      val regularAggExprs = aggExpressions.filter(!_.isDistinct)
      val regularAggChildren = regularAggExprs.flatMap(_.aggregateFunction.children).distinct
      val regularAggChildAttrMap = regularAggChildren.map(expressionAttributePair)

      // Setup aggregates for 'regular' aggregate expressions.
      val regularGroupId = Literal(0)
      val regularAggChildAttrLookup = regularAggChildAttrMap.toMap
      val regularAggOperatorMap = regularAggExprs.map { e =>
        // Perform the actual aggregation in the initial aggregate.
        val af = patchAggregateFunctionChildren(e.aggregateFunction)(regularAggChildAttrLookup)
        val operator = Alias(e.copy(aggregateFunction = af), e.sql)()

        // Select the result of the first aggregate in the last aggregate.
        val result = AggregateExpression(
          aggregate.First(evalWithinGroup(regularGroupId, operator.toAttribute), Literal(true)),
          mode = Complete,
          isDistinct = false)

        // Some aggregate functions (COUNT) have the special property that they can return a
        // non-null result without any input. We need to make sure we return a result in this case.
        val resultWithDefault = af.defaultResult match {
          case Some(lit) => Coalesce(Seq(result, lit))
          case None => result
        }

        // Return a Tuple3 containing:
        // i. The original aggregate expression (used for look ups).
        // ii. The actual aggregation operator (used in the first aggregate).
        // iii. The operator that selects and returns the result (used in the second aggregate).
        (e, operator, resultWithDefault)
      }

      // Construct the regular aggregate input projection only if we need one.
      val regularAggProjection = if (regularAggExprs.nonEmpty) {
        Seq(a.groupingExpressions ++
          distinctAggChildren.map(nullify) ++
          Seq(regularGroupId) ++
          regularAggChildren)
      } else {
        Seq.empty[Seq[Expression]]
      }

      // Construct the distinct aggregate input projections.
      val regularAggNulls = regularAggChildren.map(nullify)
      val distinctAggProjections = distinctAggOperatorMap.map {
        case (projection, _) =>
          a.groupingExpressions ++
            projection ++
            regularAggNulls
      }

      // Construct the expand operator.
      val expand = Expand(
        regularAggProjection ++ distinctAggProjections,
        groupByAttrs ++ distinctAggChildAttrs ++ Seq(gid) ++ regularAggChildAttrMap.map(_._2),
        a.child)

      // Construct the first aggregate operator. This de-duplicates the all the children of
      // distinct operators, and applies the regular aggregate operators.
      val firstAggregateGroupBy = groupByAttrs ++ distinctAggChildAttrs :+ gid
      val firstAggregate = Aggregate(
        firstAggregateGroupBy,
        firstAggregateGroupBy ++ regularAggOperatorMap.map(_._2),
        expand)

      // Construct the second aggregate
      val transformations: Map[Expression, Expression] =
      (distinctAggOperatorMap.flatMap(_._2) ++
        regularAggOperatorMap.map(e => (e._1, e._3))).toMap

      val patchedAggExpressions = a.aggregateExpressions.map { e =>
        e.transformDown {
          case e: Expression =>
            // The same GROUP BY clauses can have different forms (different names for instance) in
            // the groupBy and aggregate expressions of an aggregate. This makes a map lookup
            // tricky. So we do a linear search for a semantically equal group by expression.
            groupByMap
              .find(ge => e.semanticEquals(ge._1))
              .map(_._2)
              .getOrElse(transformations.getOrElse(e, e))
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupByAttrs, patchedAggExpressions, firstAggregate)
    } else {
      a
    }
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)

  private def expressionAttributePair(e: Expression) =
  // We are creating a new reference here instead of reusing the attribute in case of a
  // NamedExpression. This is done to prevent collisions between distinct and regular aggregate
  // children, in this case attribute reuse causes the input of the regular aggregate to bound to
  // the (nulled out) input of the distinct aggregate.
    e -> AttributeReference(e.sql, e.dataType, nullable = true)()
}
