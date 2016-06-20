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
      List(Interval.parse("1995-12-30T00:00:00.000Z/1997-12-31T00:00:01.000Z"))
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
      List(Interval.parse("1993-01-01T00:00:00.000Z/1997-08-02T00:00:00.001Z")))
  }

  test("timestamp3") { td =>
    validateFilter("l_shipdate >= '1995-12-30'",
      false)
  }

  test("timestamp4") { td =>
    validateFilter("o_orderdate >= '1995-12-30'",
      true,
      Some(
        BoundFilterSpec("bound","o_orderdate",Some("1995-12-30"),Some(false),None,None,false)
      )
    )
  }

  // scalastyle:off line.size.limit
  test("timestamp5") { td =>
    validateFilter(
      """Cast(
                        Concat(To_date(o_orderdate), ' 00:00:00')
                        AS TIMESTAMP)
                    <=  Cast('1997-08-02 00:00:00' AS TIMESTAMP)
      """,
      true,
      Some(
        new JavascriptFilterSpec("javascript",
          "o_orderdate",
          "function (o_orderdate) {\n" +
            "            \n" +
            "            var v1 = new org.joda.time.DateTimeZone.forID(\"UTC\");var v2 = org.joda.time.format.ISODateTimeFormat.dateTimeParser();var v3 = v2.withZone(v1);\n" +
            "              \n" +
            "            \n" +
            "\n" +
            "            return(((org.joda.time.DateTime.parse((((org.joda.time.LocalDate.parse(o_orderdate, v2).toString(\"yyyy-MM-dd\"))).concat(\" 00:00:00\")).replace(\" \", \"T\"), v3)).compareTo((new org.joda.time.DateTime(870480000000, v1))) <= 0));\n" +
            "            }")
      )
    )
  }


  test("monthTimestampFilter") { td =>
    validateFilter(
      """
        |Month(Cast(Concat(To_date(l_shipdate), ' 00:00:00') AS TIMESTAMP)) < 4
        | """.stripMargin,
      true,
      Some(
        new JavascriptFilterSpec("javascript",
          "__time",
          "function (__time) {\n" +
            "            \n" +
            "            var v1 = new org.joda.time.DateTimeZone.forID(\"UTC\");var v2 = org.joda.time.format.ISODateTimeFormat.dateTimeParser();var v3 = v2.withZone(v1);\n" +
            "              \n" +
            "            \n" +
            "\n" +
            "            return((org.joda.time.DateTime.parse((((new org.joda.time.LocalDate(__time, v1).toString(\"yyyy-MM-dd\"))).concat(\" 00:00:00\")).replace(\" \", \"T\"), v3).toLocalDate().getMonthOfYear())  <  (4));\n" +
            "            }")
      )
    )
  }
  // scalastyle:on

  test("jsUpper") { td =>
    validateFilter(
      """
        |upper(s_name) = 'S1'
        | """.stripMargin,
      true,
      Some(
        new JavascriptFilterSpec("javascript",
          "s_name",
          "function (s_name) {\n" +
            "            \n" +
            "            \n" +
            "            \n" +
            "            \n\n" +
            """            return(((s_name).toUpperCase())  ==  ("S1"));
            |            }""".stripMargin)
      )
    )
  }

  test("jsCoalesce") { td =>
    validateFilter(
      """
        |coalesce(s_name, 'no-supp') = 'S1'
        | """.stripMargin,
      true,
      Some(
        new JavascriptFilterSpec("javascript",
          "s_name",
          "function (s_name) {\n" +
            "            \n" +
            "            \n" +
            "            \n" +
            "            \n\n" +
            """            return((((s_name) || ("no-supp")))  ==  ("S1"));
              |            }""".stripMargin)
      )
    )
  }

}
