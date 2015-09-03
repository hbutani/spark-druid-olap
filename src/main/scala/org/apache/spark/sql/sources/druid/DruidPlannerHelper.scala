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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Alias, NamedExpression, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.DataType
import org.sparklinedata.druid.DruidOperatorAttribute


trait DruidPlannerHelper {

  /**
   *
   * @param outputAttributeMap outputAttributeMap from the QryBldr
   * @return a map from the AggregationOp output to a DruidOperatorAttribute
   */
  def buildDruidSchemaMap(outputAttributeMap:
                          Map[String, (Expression, DataType, DataType)]):
  Map[Expression, DruidOperatorAttribute] = (outputAttributeMap map {
    case (nm, (e, oDT, dDT)) => {
      val druidEid = e match {
        case n: NamedExpression => n.exprId
        case _ => NamedExpression.newExprId
      }
      (e -> DruidOperatorAttribute(druidEid,nm, dDT))
    }
  })

  def unalias(e : Expression, agg : Aggregate) : Option[Expression] = {

    agg.aggregateExpressions.find{ aE =>
      if ( (aE == e) ||
        (e.isInstanceOf[AttributeReference] &&
          e.asInstanceOf[AttributeReference].exprId == aE.exprId
          )
      ) true else false
    }.map {
      case Alias(child, _) => child
      case x => x
    }

  }



}
