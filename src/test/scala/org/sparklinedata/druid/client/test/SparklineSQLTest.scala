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

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.hive.test.sparklinedata.TestHive._

class SparklineSQLTest extends BaseTest {

  test("druidrelations") { td =>

    sql("select * from `d$druidrelations`").show(500, false)
  }

  test("druidservers") { td =>

    sql("select * from `d$druidservers`").show(500, false)
  }

  test("druidsegments") { td =>

    sql("select * from `d$druidsegments`").show(500, false)
  }

  test("druidserverassignments") { td =>

    sql("select * from `d$druidserverassignments`").show(500, false)
  }

  test("clearCache") { td =>

    sql("clear druid cache localhost").show()
  }

  test("clearCacheAll") { td =>

    sql("clear druid cache").show()
  }

  test("parseException") { td =>

    val thrown = intercept[ParseException] {
      sql("clear drud cache")
    }

    assert(thrown.getMessage() ==
    """
      |extraneous input 'drud' expecting 'CACHE'
      |SPL parse attempt message: ``druid'' expected but identifier drud found(line 1, pos 6)
      |
      |== SQL ==
      |clear drud cache
      |------^^^
      |""".stripMargin
    )
  }

  test("execQueryHistorical") { td =>

    sql(
      """on druiddatasource orderLineItemPartSupplier using historical execute query
        |{
        |    "jsonClass" : "GroupByQuerySpec",
        |    "queryType" : "groupBy",
        |    "dataSource" : "tpch",
        |    "dimensions" : [ {
        |      "jsonClass" : "DefaultDimensionSpec",
        |      "type" : "default",
        |      "dimension" : "l_returnflag",
        |      "outputName" : "l_returnflag"
        |    }, {
        |      "jsonClass" : "DefaultDimensionSpec",
        |      "type" : "default",
        |      "dimension" : "l_linestatus",
        |      "outputName" : "l_linestatus"
        |    } ],
        |    "granularity" : "all",
        |    "aggregations" : [ {
        |      "jsonClass" : "FunctionAggregationSpec",
        |      "type" : "count",
        |      "name" : "alias-1",
        |      "fieldName" : "count"
        |    }, {
        |      "jsonClass" : "FunctionAggregationSpec",
        |      "type" : "doubleSum",
        |      "name" : "alias-2",
        |      "fieldName" : "l_extendedprice"
        |    }, {
        |      "jsonClass" : "FunctionAggregationSpec",
        |      "type" : "doubleMax",
        |      "name" : "alias-3",
        |      "fieldName" : "ps_supplycost"
        |    }, {
        |      "jsonClass" : "FunctionAggregationSpec",
        |      "type" : "longSum",
        |      "name" : "alias-5",
        |      "fieldName" : "sum_ps_availqty"
        |    }, {
        |      "jsonClass" : "FunctionAggregationSpec",
        |      "type" : "count",
        |      "name" : "alias-6",
        |      "fieldName" : "count"
        |    } ],
        |    "intervals" : [ "1993-01-01T00:00:00.000Z/1997-12-31T00:00:01.000Z" ]
        |  }
      """.stripMargin).show()
  }

  test("execQuery1") { td =>

    sql(
      """ on druiddatasource orderLineItemPartSupplier execute query
        |{
        |  "jsonClass" : "GroupByQuerySpec",
        |  "queryType" : "groupBy",
        |  "dataSource" : "tpch",
        |  "dimensions" : [ {
        |    "jsonClass" : "DefaultDimensionSpec",
        |    "type" : "default",
        |    "dimension" : "l_returnflag",
        |    "outputName" : "l_returnflag"
        |  }, {
        |    "jsonClass" : "DefaultDimensionSpec",
        |    "type" : "default",
        |    "dimension" : "l_linestatus",
        |    "outputName" : "l_linestatus"
        |  }, {
        |    "jsonClass" : "ExtractionDimensionSpec",
        |    "type" : "extraction",
        |    "dimension" : "__time",
        |    "outputName" : "month",
        |    "extractionFn" : {
        |      "jsonClass" : "TimeFormatExtractionFunctionSpec",
        |      "type" : "timeFormat",
        |      "format" : "yyyy-MMM",
        |      "timeZone" : "UTC",
        |      "locale" : "en_US"
        |    }
        |  } ],
        |  "limitSpec" : {
        |    "jsonClass" : "LimitSpec",
        |    "type" : "default",
        |    "limit" : 10,
        |    "columns" : [ {
        |      "jsonClass" : "OrderByColumnSpec",
        |      "dimension" : "l_returnflag",
        |      "direction" : "ascending"
        |    }, {
        |      "jsonClass" : "OrderByColumnSpec",
        |      "dimension" : "l_linestatus",
        |      "direction" : "ascending"
        |    } ]
        |  },
        |  "granularity" : {
        |    "jsonClass" : "PeriodGranularitySpec",
        |    "type" : "period",
        |    "period" : "P1M"
        |  },
        |  "aggregations" : [ {
        |    "jsonClass" : "FunctionAggregationSpec",
        |    "type" : "longSum",
        |    "name" : "count",
        |    "fieldName" : "count"
        |  }, {
        |    "jsonClass" : "FunctionAggregationSpec",
        |    "type" : "longSum",
        |    "name" : "sum_quantity",
        |    "fieldName" : "sum_l_quantity"
        |  }, {
        |    "jsonClass" : "FunctionAggregationSpec",
        |    "type" : "doubleSum",
        |    "name" : "sum_base_price",
        |    "fieldName" : "l_extendedprice"
        |  }, {
        |    "jsonClass" : "FunctionAggregationSpec",
        |    "type" : "doubleSum",
        |    "name" : "l_discount",
        |    "fieldName" : "l_discount"
        |  }, {
        |    "jsonClass" : "FunctionAggregationSpec",
        |    "type" : "doubleSum",
        |    "name" : "l_tax",
        |    "fieldName" : "l_tax"
        |  }, {
        |    "jsonClass" : "CardinalityAggregationSpec",
        |    "type" : "cardinality",
        |    "name" : "count_order",
        |    "fieldNames" : [ "o_orderkey" ],
        |    "byRow" : true
        |  } ],
        |  "postAggregations" : [ {
        |    "jsonClass" : "ArithmeticPostAggregationSpec",
        |    "type" : "arithmetic",
        |    "name" : "sum_disc_price",
        |    "fn" : "-",
        |    "fields" : [ {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "sum_base_price"
        |    }, {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "l_discount"
        |    } ]
        |  }, {
        |    "jsonClass" : "ArithmeticPostAggregationSpec",
        |    "type" : "arithmetic",
        |    "name" : "sum_charge",
        |    "fn" : "-",
        |    "fields" : [ {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "sum_base_price"
        |    }, {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "l_tax"
        |    } ]
        |  }, {
        |    "jsonClass" : "ArithmeticPostAggregationSpec",
        |    "type" : "arithmetic",
        |    "name" : "avg_qty",
        |    "fn" : "/",
        |    "fields" : [ {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "sum_quantity"
        |    }, {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "count"
        |    } ]
        |  }, {
        |    "jsonClass" : "ArithmeticPostAggregationSpec",
        |    "type" : "arithmetic",
        |    "name" : "avg_price",
        |    "fn" : "/",
        |    "fields" : [ {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "sum_base_price"
        |    }, {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "count"
        |    } ]
        |  }, {
        |    "jsonClass" : "ArithmeticPostAggregationSpec",
        |    "type" : "arithmetic",
        |    "name" : "avg_disc",
        |    "fn" : "/",
        |    "fields" : [ {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "l_discount"
        |    }, {
        |      "jsonClass" : "FieldAccessPostAggregationSpec",
        |      "type" : "fieldAccess",
        |      "fieldName" : "count"
        |    } ]
        |  } ],
        |  "intervals" : [ "1993-01-01T00:00:00.000Z/1993-03-01T00:00:00.000Z" ]
        |}
      """.stripMargin).show()
  }

  test("explainDruidRewrite") { td =>
    sql("""explain druid rewrite
          |SELECT COUNT(DISTINCT CAST(`orderLineItemPartSupplier`.`l_shipdate` AS TIMESTAMP))
          | AS `ctd_date_string_ok`
          |FROM `orderLineItemPartSupplier`
          | HAVING (COUNT(1) > 0)
        """.stripMargin).show(Int.MaxValue - 1, false)
  }

  test("explainDruidRewrite2") { td =>
    sql("""explain druid rewrite
          |SELECT p_name, count(*)
          |FROM `orderLineItemPartSupplier`
          |group by p_name
        """.stripMargin).show(Int.MaxValue - 1, false)
  }

  test("explainDruidRewrite3") { td =>
    sql("""explain druid rewrite
          |SELECT l_quantity + 1, count(*)
          |FROM `orderLineItemPartSupplier`
          |group by l_quantity + 1
        """.stripMargin).show(Int.MaxValue - 1, false)
  }

}
