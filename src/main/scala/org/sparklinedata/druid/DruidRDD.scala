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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRowWithSchema
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.joda.time.Interval
import org.sparklinedata.druid.client.{DruidBrokerClient, QueryResultRow}
import org.sparklinedata.druid.metadata.DruidRelationInfo

class DruidPartition(idx: Int, val i : Interval) extends Partition {
  override def index: Int = idx
}

class DruidRDD(sqlContext: SQLContext,
              val drInfo : DruidRelationInfo,
                val dQuery : DruidQuery)  extends  RDD[InternalRow](sqlContext.sparkContext, Nil) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val p = split.asInstanceOf[DruidPartition]
    val client = new DruidBrokerClient(drInfo.druidClientInfo.host, drInfo.druidClientInfo.port)
    val mQry = dQuery.q.setInterval(p.i)
    Utils.logQuery(mQry)
    val dr = client.executeQueryAsStream(mQry)
    context.addTaskCompletionListener{ context => dr.closeIfNeeded() }
    val r = new InterruptibleIterator[QueryResultRow](context, dr)
    val schema = dQuery.schema(drInfo)
    r.map { r =>
      new GenericInternalRowWithSchema(schema.fields.map(f => sparkValue(f, r.event(f.name))),
        schema)
    }
  }

  override protected def getPartitions: Array[Partition] =
    dQuery.intervalSplits.zipWithIndex.map(t => new DruidPartition(t._2, t._1)).toArray

  /**
   * conversion from Druid values to Spark values. Most of the conversion cases are handled by
   * cast expressions in the [[org.apache.spark.sql.execution.Project]] operator above the
   * DruidRelation Operator; but Strings need to be converted to [[UTF8String]] strings.
    *
    * @param f
   * @param druidVal
   * @return
   */
  def sparkValue(f : StructField, druidVal : Any) : Any = f.dataType match {
    case StringType if druidVal != null => UTF8String.fromString(druidVal.toString)
    case LongType if druidVal.isInstanceOf[BigInt] => druidVal.asInstanceOf[BigInt].longValue()
    case _ => druidVal
  }
}
