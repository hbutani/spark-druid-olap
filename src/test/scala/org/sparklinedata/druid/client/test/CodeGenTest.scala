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

import org.apache.spark.sql.SPLLogging
import org.scalatest.BeforeAndAfterAll

class CodeGenTest extends BaseTest with BeforeAndAfterAll with SPLLogging {

  test("gbexprtest1",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplier group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by bal",
    1,
    true, true)
  test("gbexprtest1B",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplierBase group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by bal",
    0,
    true, true)

  test("gbexprtest2",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplier group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by o_orderdate, x, bal",
    1,
    true, true)
  test("gbexprtest2B",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by o_orderdate, x, bal",
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
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20) as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20)" +
      "order by o_orderdate, x",
    1,
    true, true)

  test("gbexprtest14B",
    "select o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20) as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
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

  test("aggTest5",
    s"""
       |SELECT avg((l_quantity + ps_availqty)/10) as x
       |FROM   orderLineItemPartSupplier
       |group by s_region
       |order by x
     """.stripMargin,
    1,
    true, true)

  test("aggTest5B",
    s"""
       |SELECT avg((l_quantity + ps_availqty)/10) as x
       |FROM   orderLineItemPartSupplierBase
       |where cast(l_shipdate as date) >= cast('1993-01-01' as date) and
       |cast(l_shipdate as date) <= cast('1997-12-30' as date)
       |group by s_region
       |order by x
     """.stripMargin,
    0,
    true, true)

  test("aggTest6",
    s"""
       |SELECT avg(unix_timestamp(l_shipdate)*1000) as x
       |FROM   orderLineItemPartSupplier
       |group by s_region
       |order by x
     """.stripMargin,
    1,
    true, true)

  test("aggTest7",
    """
      |SELECT MIN(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP)) AS x,
      |       MAX(CAST(CAST(o_orderdate AS TIMESTAMP) AS TIMESTAMP)) AS y,
      |	  COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplier ) custom_sql_query
      |   HAVING (COUNT(1) > 0)
    """.stripMargin,
    1, true, true
  )

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

  test("caseWhen",
    s"""select SUM((CASE WHEN 1000 = 0 THEN NULL ELSE CAST(l_suppkey AS DOUBLE) / 1000 END)) as x1
       |from orderLineItemPartSupplier
       |group by s_region
       |order by x1""".stripMargin,
    1,
    true, true)

  test("trunc1",
    """
      |select o_orderstatus as x, trunc(cast(o_orderdate as date), 'MM') as y
      |from orderLineItemPartSupplier
      |group by o_orderstatus, trunc(cast(o_orderdate as date), 'MM')
      |order by x, y
    """.stripMargin
    ,1,true,true)


  test("trunc1B",
    """
      |select o_orderstatus as x, trunc(cast(o_orderdate as date), 'MM') as y
      |from orderLineItemPartSupplierBase
      |group by o_orderstatus, trunc(cast(o_orderdate as date), 'MM')
      |order by x, y
    """.stripMargin
    ,0,true,true)

  test("trunc2",
    """
      |select o_orderstatus as x, trunc(cast(o_orderdate as date), 'YY') as y
      |from orderLineItemPartSupplier
      |group by o_orderstatus, trunc(cast(o_orderdate as date), 'YY')
      |order by x, y
    """.stripMargin
    ,1,true,true)


  test("trunc2B",
    """
      |select o_orderstatus as x, trunc(cast(o_orderdate as date), 'YY') as y
      |from orderLineItemPartSupplierBase
      |group by o_orderstatus, trunc(cast(o_orderdate as date), 'YY')
      |order by x, y
    """.stripMargin
    ,0,true,true)

  test("date_format1",
    """
      |select o_orderstatus as x, date_format(cast(o_orderdate as date), 'YY') as y
      |from orderLineItemPartSupplier
      |group by o_orderstatus, date_format(cast(o_orderdate as date), 'YY')
      |order by x, y
    """.stripMargin
    ,1,true,true)

  test("date_format2",
    """
      |select o_orderstatus as x, date_format(cast(o_orderdate as date), 'u') as y
      |from orderLineItemPartSupplier
      |group by o_orderstatus, date_format(cast(o_orderdate as date), 'u')
      |order by x, y
    """.stripMargin
    ,1,true,true)

  test("if1",
    """
      |select o_orderstatus as a,
      |date_format(cast(o_orderdate as date), 'u') as b,
      |if(date_format(cast(o_orderdate as date), 'u') = 1, 'true', 'false') as c
      |from orderLineItemPartSupplier
      |group by o_orderstatus, date_format(cast(o_orderdate as date), 'u'),
      |if(date_format(cast(o_orderdate as date), 'u') = 1, 'true', 'false')
      |order by a, b, c
    """.stripMargin
    ,1,true,true)

  test("if2",
    """
      |select o_orderstatus as a,
      |date_format(cast(o_orderdate as date), 'yy') as b,
      |if(date_format(cast(o_orderdate as date), 'yy') = 94, 'true', 'false') as c
      |from orderLineItemPartSupplier
      |group by o_orderstatus, date_format(cast(o_orderdate as date), 'yy'),
      |if(date_format(cast(o_orderdate as date), 'yy') = 94, 'true', 'false')
      |order by a, b, c
    """.stripMargin
    ,1,true,true)

  test("if3",
    """
      |select o_orderstatus as a,
      |date_format(cast(o_orderdate as date), 'yy') as b,
      |if(date_format(cast(o_orderdate as date), 'yy') = 94, 1, null) as c
      |from orderLineItemPartSupplier
      |group by o_orderstatus, date_format(cast(o_orderdate as date), 'yy'),
      |if(date_format(cast(o_orderdate as date), 'yy') = 94, 1, null)
      |order by a, b, c
    """.stripMargin
    ,1,true,true)

  test("substr1",
    """
      |select o_orderstatus as x, date_format(cast(o_orderdate as date), 'YYY') y,
      |cast(Substring(date_format(cast(o_orderdate as date), 'YYY'), 1,1) as int)as z
      |from orderLineItemPartSupplier
      |group by
      |o_orderstatus,
      |date_format(cast(o_orderdate as date), 'YYY'),
      |cast(Substring(date_format(cast(o_orderdate as date), 'YYY'), 1,1) as int)
      |order by x, y, z
    """.stripMargin
    ,1,true,true)

  test("substr2",
    """
      |select o_orderstatus as x, date_format(cast(o_orderdate as date), 'YYY') y,
      |cast(Substring(date_format(cast(o_orderdate as date), 'YYY'), 0,4) as int)as z
      |from orderLineItemPartSupplier
      |group by
      |o_orderstatus,
      |date_format(cast(o_orderdate as date), 'YYY'),
      |cast(Substring(date_format(cast(o_orderdate as date), 'YYY'), 0,4) as int)
      |order by x, y, z
    """.stripMargin
    ,1,true,true)

  test("substr3",
    """
      |select o_orderstatus as x, l_shipdate as y,
      |Substring(l_shipdate, -1, 2)as z
      |from orderLineItemPartSupplier
      |group by
      |o_orderstatus, l_shipdate,
      |Substring(l_shipdate, -1, 2)
      |order by x, y, z
    """.stripMargin
    ,0,true,true)

  test("substr4",
    """
      |select o_orderstatus as x, l_shipdate as y,
      |Substring(l_shipdate, 1, 0)as z
      |from orderLineItemPartSupplier
      |group by
      |o_orderstatus, l_shipdate,
      |Substring(l_shipdate, 1, 0)
      |order by x, y, z
    """.stripMargin
    ,0,true,true)

  test("substr5",
    """
      |select o_orderstatus as x, l_shipdate as y,
      |Substring(l_shipdate, 1, -2)as z
      |from orderLineItemPartSupplier
      |group by
      |o_orderstatus, l_shipdate,
      |Substring(l_shipdate, 1, -2)
      |order by x, y, z
    """.stripMargin
    ,0,true,true)

  test("substr6",
    """
      |select (SUBSTRING(c_name, 5)  = 'PROMO') as x from orderLineItemPartSupplier
      |group by (SUBSTRING(c_name, 5) = 'PROMO')
    """.stripMargin
    , 1, true, true)

  test("substr7",
    """
      |select (SUBSTRING(SUBSTRING(c_name, 1), 2)  = 'PROMO') as x from orderLineItemPartSupplier
      |group by (SUBSTRING(SUBSTRING(c_name, 1), 2) = 'PROMO')
    """.stripMargin
    , 1, true, true)

  test("subquery1",
    """
        select x, sum(z) as z from
        ( select
        Substring(o_orderstatus, 1, 2) x, Substring(l_shipdate, 1, 2) as y, c_acctbal as z
        from orderLineItemPartSupplier) r1 group by x, y
    """.stripMargin
    , 1, true, true)

  test("subquery2",
    """
        select x, sum(z) as z from
        ( select
        Substring(o_orderstatus, 1, rand()) x, Substring(l_shipdate, 1, 2) as y, c_acctbal as z
        from orderLineItemPartSupplier) r1 group by x, y
    """.stripMargin
    , 0, true, true)

  test("tscomp1",
    """
      |select sum(c_acctbal) from orderLineItemPartSupplier
      |where (CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') AS TIMESTAMP) <
      |CAST(CONCAT(TO_DATE(l_shipdate), 'T00:00:00.000Z') AS TIMESTAMP))
      |group by c_name
    """.stripMargin
    , 0, true, true)

  test("tscomp2",
    """
      |select sum(c_acctbal) from orderLineItemPartSupplier
      |where (CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') AS TIMESTAMP) <
      |CAST('1995-12-31T00:00:00.000Z'  AS TIMESTAMP))
      |group by c_name
    """.stripMargin
    , 1, true, true)

  test("tscomp3",
    """
      |select sum(c_acctbal), o_orderdate from orderLineItemPartSupplier
      |where CAST((MONTH(CAST(CONCAT(TO_DATE(o_orderdate),' 00:00:00') AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) < 2
      |group by c_name, o_orderdate
    """.stripMargin
    , 1, true, true)

  test("binaryunhex1",
    """
      |select sum(c_acctbal), o_orderdate from orderLineItemPartSupplier
      |where c_nation < concat(CAST(UNHEX('c2a3') AS string), '349- W10_HulfordsOLVPen15Sub_UK_640x480_ISV_V2_x264')
      |group by c_name, o_orderdate
    """.stripMargin
    , 1, true, true)

  test("isNull1",
    """
      |select sum(c_acctbal), c_name from orderLineItemPartSupplier
      |where l_shipdate is null
      |group by c_name
    """.stripMargin
    , 1, true, true)

  test("isNull2",
    """
      |select sum(c_acctbal), c_name from orderLineItemPartSupplier
      |where l_shipdate is null and (cast(l_shipdate as bigint) + 10) is not null
      |group by c_name
    """.stripMargin
    , 1, true, true)

  test("isNull3",
    """
      |select sum(c_acctbal), c_name from orderLineItemPartSupplier
      |where o_orderdate is not null and l_commitdate is null and (cast(l_shipdate as bigint) + 10) is not null
      |group by c_name
    """.stripMargin
    , 1, true, true)

  test("isNull4",
    """
      |select sum(c_acctbal), c_name from orderLineItemPartSupplier
      |where (o_orderdate is not null and l_commitdate is null) or ((cast(l_shipdate as bigint) + 10) is null)
      |group by c_name
    """.stripMargin
    , 1, true, true)
}

