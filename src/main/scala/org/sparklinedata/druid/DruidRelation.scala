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

package org.sparklinedata.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.joda.time.Interval
import org.sparklinedata.druid.metadata.DruidRelationInfo


case class DruidOperatorAttribute(exprId : ExprId, name : String, dataType : DataType,
                                  tf: String = null)

/**
 *
 * @param q
 * @param intervalSplits
 * @param outputAttrSpec attributes to be output from the PhysicalRDD. Each output attribute is
 *                       based on an Attribute in the originalPlan. The association is based
 *                       on the ExprId.
 */
case class DruidQuery(q : QuerySpec,
                      queryHistoricalServer : Boolean,
                      intervalSplits : List[Interval],
                       outputAttrSpec :Option[List[DruidOperatorAttribute]]
                       ) {

  def this(q : QuerySpec,
           queryHistoricalServer : Boolean = false) =
    this(q, queryHistoricalServer, q.intervalList.map(Interval.parse(_)), None)

  private def schemaFromQuerySpec(dInfo : DruidRelationInfo) : StructType = {

    val fields : List[StructField] = q.dimensions.map{d =>
      new StructField(d.outputName, d.sparkDataType(dInfo.druidDS))
    } ++
      q.aggregations.map {a =>
        new StructField(a.name, a.sparkDataType(dInfo.druidDS))
      } ++
      q.postAggregations.map{ ps =>
        ps.map {p =>
          new StructField(p.name, p.sparkDataType(dInfo.druidDS))
        }
      }.getOrElse(Nil)

    StructType(fields)
  }

  private def schemaFromOutputSpec : StructType = {
    val fields : List[StructField] = outputAttrSpec.get.map {
      case DruidOperatorAttribute(eId, nm, dT, tf) => new StructField(nm, dT)
    }
    StructType(fields)
  }

  def schema(dInfo : DruidRelationInfo) : StructType =
    outputAttrSpec.map(o => schemaFromOutputSpec).getOrElse(schemaFromQuerySpec(dInfo))

  private def outputAttrsFromQuerySpec(dInfo : DruidRelationInfo) : Seq[Attribute] = {
    schemaFromQuerySpec(dInfo).fields.map { f =>
      AttributeReference(f.name, f.dataType)()
    }
  }

  private def outputAttrsFromOutputSpec : Seq[Attribute] = {
    outputAttrSpec.get.map {
      case DruidOperatorAttribute(eId, nm, dT, tf) => AttributeReference(nm, dT)(eId)
    }
  }

  def outputAttrs(dInfo : DruidRelationInfo) : Seq[Attribute] =
    outputAttrSpec.map(o => outputAttrsFromOutputSpec).getOrElse(outputAttrsFromQuerySpec(dInfo))

  def getValTFMap(): Map[String, String] = {
    val m = new scala.collection.mutable.HashMap[String, String]
    for ( lstOA <- outputAttrSpec; oa <- lstOA if oa.tf != null) {
      m += oa.name -> oa.tf
    }
    m.toMap
  }
}

case class DruidRelation (val info : DruidRelationInfo,
                                       val dQuery : Option[DruidQuery])(
  @transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
  /*
   pass in
   - connection info to druid (host,port, dataSource, params)
   - sourceDF
   - a mapping from srcDF columns to Druid dims + metrics
   - optionally a  Druid Query
   */

  override val needConversion: Boolean = false

  override def schema: StructType =
    dQuery.map(_.schema(info)).getOrElse(info.sourceDF(sqlContext).schema)

  def buildInternalScan : RDD[InternalRow] =
    dQuery.map(new DruidRDD(sqlContext, info, _)).getOrElse(
      info.sourceDF(sqlContext).queryExecution.toRdd
    )

  override def buildScan(): RDD[Row] =
    buildInternalScan.asInstanceOf[RDD[Row]]
}
