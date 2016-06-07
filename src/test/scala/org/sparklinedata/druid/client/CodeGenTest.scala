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

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll

class CodeGenTest extends BaseTest with BeforeAndAfterAll with Logging {

  test("gbexprtest1",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplier group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal",
    1,
    true, true)
  test("gbexprtest1B",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplierBase group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal",
    0,
    true, true)

  test("gbexprtest2",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplier group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by o_orderdate, x, bal",
    1,
    true, true)
  test("gbexprtest2B",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by o_orderdate, x, bal",
    0,
    true, true)
  test("gbexprtest3",
    "select o_orderdate, " +
      "(DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) as x " +
      "from orderLineItemPartSupplier group by o_orderdate, " +
      "cast('2015-07-21 10:10:10 PST' as date), " +
      " (DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) " +
      "order by o_orderdate, x",
    1,
    true, true)
  test("gbexprtest3B",
    "select o_orderdate, " +
      "(DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) as x " +
      "from orderLineItemPartSupplierBase group by o_orderdate, " +
      "cast('2015-07-21 10:10:10 PST' as date), " +
      " (DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) " +
      "order by o_orderdate, x",
    0,
    true, true)
  test("gbexprtest4",
    "select (Date_add(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplier group by " +
      "(Date_add(cast(o_orderdate as date), 360+3)) order by x",
    1,
    true, true)
  test("gbexprtest4B",
    "select (Date_add(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplierBase group by " +
      "(Date_add(cast(o_orderdate as date), 360+3)) order by x",
    0,
    true, true)
  test("gbexprtest5",
    "select (Date_sub(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplier group by " +
      "(Date_sub(cast(o_orderdate as date), 360+3)) order by x",
    1,
    true, true)
  test("gbexprtest5B",
    "select (Date_sub(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplierBase group by " +
      "(Date_sub(cast(o_orderdate as date), 360+3)) order by x",
    0,
    true, true)
  test("gbexprtest6",
    "select o_orderdate, (weekofyear(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplier group by o_orderdate, " +
      "(weekofyear(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate",
    1,
    true, true)
  test("gbexprtest6B",
    "select o_orderdate, (weekofyear(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplierBase group by o_orderdate, " +
      "(weekofyear(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate",
    0,
    true, true)
  test("gbexprtest7",
    "select o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplier group by o_orderdate, " +
      "(unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate, x",
    1,
    true, true)
  test("gbexprtest7B",
    "select o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplierBase group by o_orderdate, " +
      "(unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate, x",
    0,
    true, true)

  test("gbexprtest8",
    "SELECT   o_orderdate, Cast(Concat(Year(Cast(o_orderdate AS TIMESTAMP)), " +
      "(CASE WHEN Month(Cast(o_orderdate AS TIMESTAMP))<4 " +
      "THEN '-01' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<7 " +
      "THEN '-04' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<10 " +
      "THEN '-07' ELSE '-10'  END), '-01 00:00:00') " +
      "AS TIMESTAMP) AS x " +
      "FROM (SELECT * FROM   orderLineItemPartSupplier) m " +
      "GROUP BY o_orderdate, cast(concat(year(cast(o_orderdate AS timestamp)), " +
      "(CASE WHEN month(cast(o_orderdate AS timestamp))<4 THEN '-01' " +
      "WHEN month(cast(o_orderdate AS timestamp))<7 THEN '-04' " +
      "WHEN month(cast(o_orderdate AS timestamp))<10 " +
      "THEN '-07' ELSE '-10' END), '-01 00:00:00') AS timestamp)" +
      " order by o_orderdate, x ",
    1,
    true, true)
  test("gbexprtest8B",
    "SELECT   o_orderdate, Cast(Concat(Year(Cast(o_orderdate AS TIMESTAMP)), " +
      "(CASE WHEN Month(Cast(o_orderdate AS TIMESTAMP))<4 " +
      "THEN '-01' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<7 " +
      "THEN '-04' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<10 " +
      "THEN '-07' ELSE '-10'  END), '-01 00:00:00') " +
      "AS TIMESTAMP) AS x " +
      "FROM (SELECT * FROM   orderLineItemPartSupplierBase) m " +
      "GROUP BY o_orderdate, cast(concat(year(cast(o_orderdate AS timestamp)), " +
      "(CASE WHEN month(cast(o_orderdate AS timestamp))<4 THEN '-01' " +
      "WHEN month(cast(o_orderdate AS timestamp))<7 THEN '-04' " +
      "WHEN month(cast(o_orderdate AS timestamp))<10 " +
      "THEN '-07' ELSE '-10' END), '-01 00:00:00') AS timestamp)" +
      " order by o_orderdate, x ",
    0,
    true, true)

  test("gbexprtest9",
    "select o_orderdate as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) " +
      "order by o_orderdate, x",
    1,
    true, true)

  test("gbexprtest9B",
    "select o_orderdate as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1)))  " +
      "order by o_orderdate, x",
    0,
    true, true)


  test("gbexprtest10",
    " SELECT CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(o_orderdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplier) custom_sql_query " +
      "GROUP BY  " +
      "CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT), " +
      "YEAR(CAST(o_orderdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    1,
    true, true)

  test("gbexprtest10B",
    " SELECT CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(o_orderdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplierBase) custom_sql_query " +
      "GROUP BY  " +
      "CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT), " +
      "YEAR(CAST(o_orderdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    0,
    true, true)

  test("gbexprtest11",
    " SELECT CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(l_shipdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplier) custom_sql_query " +
      "GROUP BY  " +
      "CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT), " +
      "YEAR(CAST(l_shipdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    1,
    true, true)
  test("gbexprtest11B",
    " SELECT CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(l_shipdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplierBase) custom_sql_query " +
      "GROUP BY  " +
      "CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT), " +
      "YEAR(CAST(l_shipdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    0,
    true, true)

  test("gbexprtest12",
    "select o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1)))) as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1))))  " +
      "order by o_orderdate, x",
    1,
    true, true)
  test("gbexprtest12B",
    "select o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1)))) as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1))))  " +
      "order by o_orderdate, x",
    0,
    true, true)

  test("gbexprtest13",
    "select o_orderdate, " +
      "datediff(" +
      "date_add(to_date(cast(o_orderdate as timestamp)), (year(cast(o_orderdate as date))%100)), " +
      "date_sub(to_date(cast(o_orderdate as timestamp)), quarter(cast(o_orderdate as date))*2)" +
      ") as c1, " +
      "datediff( " +
      "date_add(to_date(cast(o_orderdate as timestamp)), month(cast(o_orderdate as date))), " +
      "date_sub(to_date(cast(o_orderdate as timestamp)), weekofyear(cast(o_orderdate as date)))" +
      ")as c2, " +
      "datediff( " +
      "date_add(to_date(cast(o_orderdate as timestamp)), day(cast(o_orderdate as date))), " +
      "date_sub(to_date(cast(o_orderdate as timestamp)), hour(cast(o_orderdate as date))+10))" +
      "as c3, " +
      "datediff( " +
      "date_add(to_date(cast(o_orderdate as timestamp)), minute(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), second(cast(o_orderdate as date))+10))" +
      " as c4 " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, " +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "year(cast(o_orderdate as date))%100)," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), quarter(cast(o_orderdate as date))*2))," +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "month(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), weekofyear(cast(o_orderdate as date))))," +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "day(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), hour(cast(o_orderdate as date))+10))," +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "minute(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), second(cast(o_orderdate as date))+10)) " +
      "order by o_orderdate, c1, c2, c3, c4",
    1,
    true, true)

  test("gbexprtest13B",
    "select o_orderdate, " +
      "datediff(" +
      "date_add(to_date(cast(o_orderdate as timestamp)), (year(cast(o_orderdate as date))%100)), " +
      "date_sub(to_date(cast(o_orderdate as timestamp)), quarter(cast(o_orderdate as date))*2)" +
      ") as c1, " +
      "datediff( " +
      "date_add(to_date(cast(o_orderdate as timestamp)), month(cast(o_orderdate as date))), " +
      "date_sub(to_date(cast(o_orderdate as timestamp)), weekofyear(cast(o_orderdate as date)))" +
      ")as c2, " +
      "datediff( " +
      "date_add(to_date(cast(o_orderdate as timestamp)), day(cast(o_orderdate as date))), " +
      "date_sub(to_date(cast(o_orderdate as timestamp)), hour(cast(o_orderdate as date))+10))" +
      "as c3, " +
      "datediff( " +
      "date_add(to_date(cast(o_orderdate as timestamp)), minute(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), second(cast(o_orderdate as date))+10))" +
      " as c4 " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, " +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "year(cast(o_orderdate as date))%100)," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), quarter(cast(o_orderdate as date))*2))," +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "month(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), weekofyear(cast(o_orderdate as date))))," +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "day(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), hour(cast(o_orderdate as date))+10))," +
      "datediff(date_add(to_date(cast(o_orderdate as timestamp)), " +
      "minute(cast(o_orderdate as date)))," +
      "date_sub(to_date(cast(o_orderdate as timestamp)), second(cast(o_orderdate as date))+10)) " +
      "order by o_orderdate, c1, c2, c3, c4",
    0,
    true, true)

  test("gbexprtest14",
    "select o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 0, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 11, 8))) as date)," +
      " 20) as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 0, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 11, 8))) as date)," +
      " 20)" +
      "order by o_orderdate, x",
    1,
    true, true)

  test("gbexprtest14B",
    "select o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 0, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 11, 8))) as date)," +
      " 20) as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 0, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 11, 8))) as date)," +
      " 20)" +
      "order by o_orderdate, x",
    0,
    true, true)

  test("gbexprtest15",
    "select o_orderdate, " +
      "Coalesce((CASE WHEN Month(Cast(o_orderdate AS date)) > 0  and " +
      "Month(Cast(o_orderdate AS date)) < 4 THEN \"Q1\" else null END), " +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) >= 4  and Month(Cast(o_orderdate AS date))" +
      " <= 6  THEN \"Q2\" else null END), (CASE WHEN Month(Cast(o_orderdate AS date)) > 6" +
      "  and Month(Cast(o_orderdate AS date)) <= 8  THEN \"Q3\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) <> 12    THEN \"Not Dec\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) = 12    THEN \"Dec\" else null END))as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, " +
      "Coalesce((CASE WHEN Month(Cast(o_orderdate AS date)) > 0  and " +
      "Month(Cast(o_orderdate AS date)) < 4 THEN \"Q1\" else null END),  " +
      "(CASE WHEN Month(Cast(o_orderdate AS date)) >= 4  and " +
      "Month(Cast(o_orderdate AS date)) <= 6  THEN \"Q2\" else null END), " +
      "(CASE WHEN Month(Cast(o_orderdate AS date)) > 6  and " +
      "Month(Cast(o_orderdate AS date)) <= 8  THEN \"Q3\" else null END), " +
      "(CASE WHEN Month(Cast(o_orderdate AS date)) <> 12    " +
      "THEN \"Not Dec\" else null END), (CASE WHEN Month(Cast(o_orderdate AS date)) = 12    " +
      "THEN \"Dec\" else null END))" +
      "order by o_orderdate, x",
    1,
    true, true)

  test("gbexprtest15B",
    "select o_orderdate, " +
      "Coalesce((CASE WHEN Month(Cast(o_orderdate AS date)) > 0  and " +
      "Month(Cast(o_orderdate AS date)) < 4 THEN \"Q1\" else null END), " +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) >= 4  and Month(Cast(o_orderdate AS date))" +
      " <= 6  THEN \"Q2\" else null END), (CASE WHEN Month(Cast(o_orderdate AS date)) > 6" +
      "  and Month(Cast(o_orderdate AS date)) <= 8  THEN \"Q3\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) <> 12    THEN \"Not Dec\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) = 12    THEN \"Dec\" else null END))as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, " +
      "Coalesce((CASE WHEN Month(Cast(o_orderdate AS date)) > 0  and " +
      "Month(Cast(o_orderdate AS date)) < 4 THEN \"Q1\" else null END),  " +
      "(CASE WHEN Month(Cast(o_orderdate AS date)) >= 4  and " +
      "Month(Cast(o_orderdate AS date)) <= 6  THEN \"Q2\" else null END), " +
      "(CASE WHEN Month(Cast(o_orderdate AS date)) > 6  and " +
      "Month(Cast(o_orderdate AS date)) <= 8  THEN \"Q3\" else null END), " +
      "(CASE WHEN Month(Cast(o_orderdate AS date)) <> 12    " +
      "THEN \"Not Dec\" else null END), (CASE WHEN Month(Cast(o_orderdate AS date)) = 12    " +
      "THEN \"Dec\" else null END))" +
      "order by o_orderdate, x",
    0,
    true, true)

  test("aggTest1",
    """
      |SELECT MIN(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS x,
      |       MAX(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS y,
      |	  COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplier ) custom_sql_query
      |   HAVING (COUNT(1) > 0)
    """.stripMargin,
    1, true, true
  )

  test("aggTest1B",
    """
      |SELECT MIN(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS x,
      |       MAX(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS y,
      |	  COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplierBase ) custom_sql_query
      |   HAVING (COUNT(1) > 0)
    """.stripMargin,
    0, true, true
  )

  test("aggTest2",
    """
      |SELECT sum(l_quantity + 10) as s, MIN(l_quantity + 10) AS mi,
      |          MAX(l_quantity + 10) ma, COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplier ) custom_sql_query
      |   HAVING (COUNT(1) > 0) order by s, mi, ma
    """.stripMargin,
    1, true, true
  )

  test("aggTest2B",
    """
      |SELECT sum(l_quantity + 10) as s, MIN(l_quantity + 10) AS mi,
      |          MAX(l_quantity + 10) ma, COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplierBase ) custom_sql_query
      |   HAVING (COUNT(1) > 0) order by s, mi, ma
    """.stripMargin,
    0, true, true
  )

  test("aggTest3",
    s"""
        |SELECT Min(Cast(Concat(To_date(Cast(Concat(To_date(l_shipdate),' 00:00:00')
        |AS TIMESTAMP)),' 00:00:00') AS TIMESTAMP)) AS mi,
        |max(cast(concat(to_date(cast(concat(to_date(l_shipdate),' 00:00:00') AS timestamp)),
        |' 00:00:00') AS timestamp)) AS ma,
        |count(1) as c
        |FROM   orderLineItemPartSupplier
        |HAVING (count(1) > 0)
     """.stripMargin,
    1,
    true, true)

  test("aggTest3B",
    s"""
       |SELECT Min(Cast(Concat(To_date(Cast(Concat(To_date(l_shipdate),' 00:00:00')
       |AS TIMESTAMP)),' 00:00:00') AS TIMESTAMP)) AS mi,
       |max(cast(concat(to_date(cast(concat(to_date(l_shipdate),' 00:00:00') AS timestamp)),
       |' 00:00:00') AS timestamp)) AS ma,
       |count(1) as c
       |FROM   orderLineItemPartSupplierBase
       |HAVING (count(1) > 0)
     """.stripMargin,
    0,
    true, true)

  test("aggTest4",
    s"""
       |SELECT max(cast(FROM_UNIXTIME(unix_timestamp(l_shipdate)*1000, 'yyyy-MM-dd 00:00:00') as timestamp))
       |FROM   orderLineItemPartSupplier
       |group by s_region
     """.stripMargin,
    1,
    true, true)

  test("pmod1",
    """
      |SELECT max(pmod(o_totalprice, -5)) as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("pmod1B",
    """
      |SELECT max(pmod(o_totalprice, -5)) as s
      |   FROM orderLineItemPartSupplierBase
      |   group by s_region
      |   order by s
    """.stripMargin,
    0, true, true
  )

  test("pmod2",
    """
      |SELECT max(pmod(-5, o_totalprice)) as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("pmod2B",
    """
      |SELECT max(pmod(-5, o_totalprice)) as s
      |   FROM orderLineItemPartSupplierBase
      |   group by s_region
      |   order by s
    """.stripMargin,
    0, true, true
  )

  test("abs1",
    """
      |SELECT sum(abs(o_totalprice * -5)) as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("floor1",
    """
      |SELECT sum(floor(o_totalprice/3.5))  as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("ceil1",
    """
      |SELECT sum(ceil(o_totalprice/3.5))  as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("sqrt1",
    """
      |SELECT sum(sqrt(o_totalprice)) as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("Log1",
    """
      |SELECT sum(Log(o_totalprice)) as s
      |   FROM orderLineItemPartSupplier
      |   group by s_region
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("simplifyCastTst1",
    """
      |SELECT sum(Log(o_totalprice)) as s
      |   FROM orderLineItemPartSupplier
      |   group by
      |   cast(l_shipdate as int)
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("unaryMinus1",
    """
      |SELECT
      | cast(concat(date_add(cast(l_shipdate AS timestamp),
      | cast(-((1 + pmod(datediff(to_date(cast(l_shipdate AS timestamp)),
      | '1995-01-01'), 7)) - 1) AS int)),' 00:00:00') AS timestamp)
      |as s
      |   FROM orderLineItemPartSupplier
      |   group by
      |   cast(concat(date_add(cast(l_shipdate AS timestamp),
      |   cast(-((1 + pmod(datediff(to_date(cast(l_shipdate AS timestamp)), '1995-01-01'), 7))
      |    - 1) AS int)),' 00:00:00') AS timestamp)
      |   order by s
    """.stripMargin,
    1, true, true
  )
  test("unaryPlus1",
    """
      |SELECT
      | cast(concat(date_add(cast(l_shipdate AS timestamp),
      | cast(+((1 + pmod(datediff(to_date(cast(l_shipdate AS timestamp)),
      | '1995-01-01'), 7)) - 1) AS int)),' 00:00:00') AS timestamp)
      |as s
      |   FROM orderLineItemPartSupplier
      |   group by
      |   cast(concat(date_add(cast(l_shipdate AS timestamp),
      |   cast(+((1 + pmod(datediff(to_date(cast(l_shipdate AS timestamp)), '1995-01-01'), 7))
      |    - 1) AS int)),' 00:00:00') AS timestamp)
      |   order by s
    """.stripMargin,
    1, true, true
  )

  test("strGTLTEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where (o_orderdate <= '1993-12-12') and
      |(o_orderdate >= '1993-10-12')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,1,true,true)
  test("strGTLTEq1B",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where (o_orderdate <= '1993-12-12') and
      |(o_orderdate >= '1993-10-12')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,0,true,true)
  test("dateEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where cast(o_orderdate as date) = cast('1994-06-30' as Date)
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,1,true,true)
  test("dateEq1B",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where cast(o_orderdate as date) = cast('1994-06-30' as Date)
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,0,true,true)
  test("dateGTLTEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where (cast(o_orderdate as date) <= cast('1993-12-12' as Date))
      | and (cast(o_orderdate as date) >= cast('1993-10-12' as Date))
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,1,true,true)
  test("dateGTLTEq1B",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where (cast(o_orderdate as date) <= cast('1993-12-12' as Date))
      | and (cast(o_orderdate as date) >= cast('1993-10-12' as Date))
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,0,true,true)

  test("tsEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where cast(l_shipdate as timestamp) = cast('1996-05-17T17:00:00.000-07:00' as timestamp)
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,1,true,true)

  test("tsGTLTEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where (cast(l_shipdate as timestamp) <= cast('1993-12-12 00:00:00' as timestamp))
      | and (cast(l_shipdate as timestamp) >= cast('1993-10-12 00:00:00' as timestamp))
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,1,true,true)
  test("tsGTLTEq1B",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where (cast(l_shipdate as timestamp) <= cast('1993-12-12 00:00:00' as timestamp))
      | and (cast(l_shipdate as timestamp) >= cast('1993-10-12 00:00:00' as timestamp))
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
    ,0,true,true)
  test("inclause-insetTest1",
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplier
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate) <= cast('1997-12-31' as date)
      and cast(order_year as int) in (1985,1986,1987,1988,1989,1990,1991,1992,
      1993,1994,1995,1996,1997,1998,1999,2000, null)
      group by c_name
      order by c_name, bal""".stripMargin,
    1,
    true, true)
  test("inclause-insetTest1B",
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplierBase
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate) <= cast('1997-12-31' as date)
      and cast(order_year as int) in (1985,1986,1987,1988,1989,1990,1991,1992,
      1993,1994,1995,1996,1997,1998,1999,2000, null)
      group by c_name
      order by c_name, bal""".stripMargin,
    0,
    true, true)
  test("inclause-inTest1",
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplier
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate) <= cast('1997-12-31' as date)
       and cast(order_year as int) in (1993,1994,1995, null)
      group by c_name
      order by c_name, bal""".stripMargin,
    1,
    true, true)
  test("inclause-inTest1B",
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplierBase
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate) <= cast('1997-12-31' as date)
       and cast(order_year as int) in (1993,1994,1995, null)
      group by c_name
      order by c_name, bal""".stripMargin,
    0,
    true, true)
}
