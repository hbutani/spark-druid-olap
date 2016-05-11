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

import com.github.nscala_time.time.Imports._
import org.sparklinedata.druid._


class FilterPlanningTest extends PlanningTest {
  test("in") { td =>
    validateFilter("c_mktsegment in ('MACHINERY', 'HOUSEHOLD')",
      true,
      Some(
        ExtractionFilterSpec("extraction",
          "c_mktsegment",
          "true",
          InExtractionFnSpec("lookup",
            LookUpMap("map",
              Map("MACHINERY" -> "true", "HOUSEHOLD" -> "true")
            )
          )
        )
      )
    )
  }

  test("timestamp1") { td =>
    validateFilter("Cast(l_shipdate  AS timestamp) >= Cast('1995-12-30' AS timestamp)",
      true,
      None,
      List(Interval.parse("1995-12-30T00:00:00.000-08:00/1998-01-01T00:00:00.000-08:00"))
    )
  }

  test("timestamp2") { td =>
    validateFilter(
      """Cast(
                        Concat(To_date(l_shipdate), ' 00:00:00')
                        AS TIMESTAMP)
                    <=  Cast('1997-08-02 00:00:00' AS TIMESTAMP)
      """,
      true,
      None,
      List(Interval.parse("1993-01-01T00:00:00.000-08:00/1997-08-02T00:00:00.001-07:00")))
  }

  test("timestamp3") { td =>
    validateFilter("l_shipdate >= '1995-12-30'",
      false)
  }

  test("timestamp4") { td =>
    validateFilter("o_orderdate >= '1995-12-30'",
      true,
      Some(
        JavascriptFilterSpec(
          "javascript",
          "o_orderdate",
          "function(x) { return(x >= '1995-12-30') }"
        )
      )
    )
  }

  test("monthTimestampFilter") { td =>
    validateFilter(
      """
        |Month(Cast(Concat(To_date(l_shipdate), ' 00:00:00') AS TIMESTAMP)) < 4
        | """.stripMargin,
      false)
  }

}
