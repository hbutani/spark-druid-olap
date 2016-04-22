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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.types.DataType
import org.sparklinedata.druid.{DruidOperatorAttribute, DruidQueryBuilder}

/**
  * For a [[DruidQueryBuilder]] provides the various output  attribute lists.
  * @param dqb
  */
class DruidOperatorSchema(val dqb : DruidQueryBuilder) {

  def avgExpressions = dqb.avgExpressions

  /**
    * The Attributes calculated in Druid.
    */
  lazy val operatorDruidAttributes : List[DruidOperatorAttribute] = druidAttrMap.values.toList

  /**
    * The output schema for the PhysicalScan that wraps the Druid Query.
    */
  lazy val operatorSchema : List[Attribute] = operatorDruidAttributes.map {
    case DruidOperatorAttribute(eId, nm, dT) => AttributeReference(nm, dT)(eId)
  }

  /**
    * Map from Name to DruidOperatorAttribute.
    */
  lazy val druidAttrMap : Map[String, DruidOperatorAttribute] =
    buildDruidOpAttr

  /**
    * A map from the Aggregate Expressions pushed to Druid to the DruidOperatorAttribute
    * in the Druid Query output.
    */
  lazy val pushedDownExprToDruidAttr : Map[Expression, DruidOperatorAttribute] =
    buildPushDownDruidAttrsMap

  private def pushDownExpressionMap : Map[String, (Expression, DataType, DataType)] =
    dqb.outputAttributeMap.filter(t => t._2._1 != null)

  private def buildPushDownDruidAttrsMap : Map[Expression, DruidOperatorAttribute] =
    (pushDownExpressionMap map {
    case (nm, (e, oDT, dDT)) => {
      (e -> druidAttrMap(nm))
    }
  })


  private def buildDruidOpAttr : Map[String, DruidOperatorAttribute] =
    (dqb.outputAttributeMap map {
      case (nm, (e, oDT, dDT)) => {
        val druidEid = e match {
          case null => NamedExpression.newExprId
          case n: NamedExpression => n.exprId
          case _ => NamedExpression.newExprId
        }
        (nm -> DruidOperatorAttribute(druidEid, nm, dDT))
      }
    }
      )

}
