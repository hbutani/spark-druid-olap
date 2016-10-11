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
import org.sparklinedata.druid._

import scala.language.reflectiveCalls

class QueryExtTest extends AbstractTest {


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

    def zcDruidDS(db : String = "default",
                  table : String = "zipCodesBase",
                  dsName : String = "zipCodes"
                 ) =
      s"""CREATE TABLE if not exists $dsName
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "$db.$table",
      timeDimensionColumn "record_date",
      druidDatasource "zipCodes",
      druidHost '$zkConnectString',
      zkQualifyDiscoveryNames "true",
      numProcessingThreadsPerHistorical '1',
      allowTopNRewrite "true")""".stripMargin

    val cT = zipCodesTable

    println(cT)
    sql(cT)

    TestHive.table("zipCodesBase").cache()

    TestHive.setConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK.key,
      "zipCodesBase")

    // sql("select * from orderLineItemPartSupplierBase limit 10").show(10)

    val cTOlap = zcDruidDS()

    println(cTOlap)
    sql(cTOlap)

  }

  test("1") { td =>

    // Thread.sleep(120 * 60 * 1000)
    sql("select count(*) from zipCodesBase").show()
    sql("select count(*) from zipCodes").show()
    println("done")


  }

}

