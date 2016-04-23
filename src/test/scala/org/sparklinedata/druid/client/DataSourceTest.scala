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

package org.sparklinedata.druid.client

import org.apache.spark.sql.hive.test.TestHive._
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.DruidQuery
import org.sparklinedata.druid.metadata.{FunctionalDependency, FunctionalDependencyType}

class DataSourceTest extends BaseTest {

  import org.sparklinedata.druid.Utils._

  test("baseTable") { td =>
    sql("select * from orderLineItemPartSupplierBase").show(10)
  }

  test("noQuery") { td =>
    val df = sql("select * from orderLineItemPartSupplier")
    df.explain(true)
    df.show(10)
  }

  test("tpchQ1") { td =>

    val dq =
      compact(render(Extraction.decompose(new DruidQuery(TPCHQueries.q1)))).replace('\n', ' ')

    val q = s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier2
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema',
      druidQuery '$dq')""".stripMargin

    println(q)

    sql(q)

    sql("select * from orderLineItemPartSupplier2").show(10)
  }

  test("tpchQ1MonthGrain") { td =>

    val dq =
      compact(render(Extraction.decompose(new DruidQuery(TPCHQueries.q1MonthGrain)))
      ).replace('\n', ' ')

    sql(

      s"""CREATE TEMPORARY TABLE orderLineItemPartSupplier2
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema',
      druidQuery '$dq')""".stripMargin
    )

    sql("select * from orderLineItemPartSupplier2").show(10)
  }

  test("t2") { td =>
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val fd = FunctionalDependency("a", "b", FunctionalDependencyType.OneToOne)
    println(pretty(render(Extraction.decompose(fd))))
  }


}
