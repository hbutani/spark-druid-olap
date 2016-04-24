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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.SparkPlan
import org.sparklinedata.druid._

class PostAggregate(val druidOpSchema : DruidOperatorSchema) {

  val dqb = druidOpSchema.dqb

  private def attrRef(dOpAttr : DruidOperatorAttribute) : AttributeReference =
    AttributeReference(dOpAttr.name, dOpAttr.dataType)(dOpAttr.exprId)

  lazy val groupExpressions = dqb.dimensions.map { d =>
    attrRef(druidOpSchema.druidAttrMap(d.outputName))

  }

  def namedGroupingExpressions = groupExpressions

  private def toSparkAgg(dAggSpec : AggregationSpec) : Option[AggregateFunction] = {
    val dOpAttr = druidOpSchema.druidAttrMap(dAggSpec.name)
    dAggSpec match
    {
      case FunctionAggregationSpec("count", nm, _) => Some(Sum(attrRef(dOpAttr)))
      case FunctionAggregationSpec("longSum", nm, _) => Some(Sum(attrRef(dOpAttr)))
      case FunctionAggregationSpec("doubleSum", nm, _) => Some(Sum(attrRef(dOpAttr)))
      case FunctionAggregationSpec("longMin", nm, _) => Some(Min(attrRef(dOpAttr)))
      case FunctionAggregationSpec("doubleMin", nm, _) => Some(Min(attrRef(dOpAttr)))
      case FunctionAggregationSpec("longMax", nm, _) => Some(Max(attrRef(dOpAttr)))
      case FunctionAggregationSpec("doubleMax", nm, _) => Some(Max(attrRef(dOpAttr)))
      case _ => None
    }
  }

  lazy val aggregatesO : Option[List[NamedExpression]] = Utils.sequence(
    dqb.aggregations.map { da =>
    val dOpAttr = druidOpSchema.druidAttrMap(da.name)
    toSparkAgg(da).map { aggFunc =>
      Alias(AggregateExpression(aggFunc, Complete, false), dOpAttr.name)(dOpAttr.exprId)
    }
  })

  def canBeExecutedInHistorical : Boolean = dqb.canPushToHistorical && aggregatesO.isDefined

  lazy val resultExpressions = groupExpressions ++ aggregatesO.get

  lazy val aggregateExpressions = resultExpressions.flatMap { expr =>
    expr.collect {
      case agg: AggregateExpression => agg
    }
  }.distinct

  lazy val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
    val aggregateFunction = agg.aggregateFunction
    val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
    (aggregateFunction, agg.isDistinct) -> attribute
  }.toMap

  lazy val rewrittenResultExpressions = resultExpressions.map { expr =>
    expr.transformDown {
      case AggregateExpression(aggregateFunction, _, isDistinct) =>
        // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
        // so replace each aggregate expression by its corresponding attribute in the set:
        aggregateFunctionToAttribute(aggregateFunction, isDistinct)
      case expression => expression
    }.asInstanceOf[NamedExpression]
  }

  def aggOp(child : SparkPlan) : Seq[SparkPlan] = {
    org.apache.spark.sql.execution.aggregate.Utils.planAggregateWithoutPartial(
      namedGroupingExpressions,
        aggregateExpressions,
        aggregateFunctionToAttribute,
        rewrittenResultExpressions,
      child)
  }

}
