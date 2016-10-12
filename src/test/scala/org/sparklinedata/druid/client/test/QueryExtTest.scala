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

package org.sparklinedata.druid.client.test

import java.io.File

import org.apache.spark.sql.hive.test.sparklinedata.TestHive
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.json4s.Extraction
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.{DruidRelationColumnInfo, SpatialDruidDimensionInfo}

import scala.language.reflectiveCalls
import scala.reflect.ClassTag

abstract class QueryExtTest extends AbstractTest {

  def hasAgg[T <: AggregationSpec : ClassTag](dq : DruidQuery) : Boolean = {
    if ( !dq.q.isInstanceOf[AggQuerySpec]) return false
    val aggQ = dq.q.asInstanceOf[AggQuerySpec]
    aggQ.aggregations.find {
      case a:T => true
      case _ => false
    }.isDefined
  }

  def hasHLLAgg(dq : DruidQuery) : Boolean = hasAgg[HyperUniqueAggregationSpec](dq)
  def hasCardinalityAgg(dq : DruidQuery) : Boolean = hasAgg[CardinalityAggregationSpec](dq)

  override def beforeAll() = {

    super.beforeAll()

    def zipCodeIndexTask(idxTemplate : String,
                  taskName : String) : File = {
      Utils.createTempFileFromTemplate(
        idxTemplate,
        Map(
          ":DATA_DIR:" ->
            "src/test/resources/zipCodes/sample"
        ),
        taskName,
        "json"
      )
    }

    val zipCodesTask = zipCodeIndexTask(
      "zip_code.json.template",
      "zipcode_index_task"
    )

    val zipCodeAllsTask = zipCodeIndexTask(
      "zip_codeAll.json.template",
      "zipcodeAll_index_task"
    )

    ensureDruidIndex("zipCodes", zipCodesTask)(15, 1000 * 1, 1500 * 10)
    ensureDruidIndex("zipCodesAll", zipCodeAllsTask)(15, 1000 * 1, 1500 * 10)

    val zipCodesTable =
      s"""CREATE TABLE if not exists zipCodesBase(
          record_date string,
          |zip_code string,
          |latitude double,
          |longitude double,
          |city string,
          |state string,
          |county string
             )
      USING com.databricks.spark.csv
      OPTIONS (path "src/test/resources/zipCodes/sample/zip_codes_states.csv",
      header "false", delimiter ",")""".stripMargin

    def zcDruidDSColInfos(isFull : Boolean) = {
      val l = List(
        DruidRelationColumnInfo(
          "city",
          if (isFull) Some("city") else None,
          None,
          Some("unique_city"),
          Some("city_sketch")
        ),
        DruidRelationColumnInfo(
          "latitude",
          // should be if (isFull) Some("latitude") else None, but Druid doesn't expose latitude
          if (isFull) None else None,
          Some(SpatialDruidDimensionInfo("coordinates", 0, Some(-90.0), Some(90.0)))
        ),
        DruidRelationColumnInfo(
          "longitude",
          // should be if (isFull) Some("longitude") else None, but Druid doesn't expose longitude
          if (isFull) None else None,
          Some(SpatialDruidDimensionInfo("coordinates", 1, Some(-180.0), Some(180.0)))
        )
      )
      import Utils._
      import org.json4s.jackson.JsonMethods._

      val jR = render(Extraction.decompose(l))
      pretty(jR).stripMargin.replace('\n', ' ')
    }

    def zcDruidDS(isFull : Boolean = false,
                  db : String = "default",
                  table : String = "zipCodesBase",
                  dsName : String = "zipCodes"
                 ) =
      s"""CREATE TABLE if not exists ${if (isFull) dsName + "Full" else dsName}
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "$db.$table",
      timeDimensionColumn "record_date",
      druidDatasource "${if (isFull) "zipCodesAll" else "zipCodes"}",
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      numProcessingThreadsPerHistorical '1',
      columnInfos '${zcDruidDSColInfos(isFull)}',
      nonAggregateQueryHandling "push_project_and_filters",
      allowTopNRewrite "true")""".stripMargin

    val cT = zipCodesTable

    println(cT)
    sql(cT)

    TestHive.table("zipCodesBase").cache()

    TestHive.setConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK.key,
      "zipCodesBase")

    // sql("select * from orderLineItemPartSupplierBase limit 10").show(10)

    var cTOlap = zcDruidDS()
    println(cTOlap)
    sql(cTOlap)

    cTOlap = zcDruidDS(true)
    println(cTOlap)
    sql(cTOlap)


  }

}

