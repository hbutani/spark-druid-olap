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

class CodeGenCorrectnessTest extends BaseTest with BeforeAndAfterAll with Logging {

  cTest("gbexprtest1",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplier where l_shipdate >= " +
      "'1994-01-01' " +
      " and  l_shipdate <= '1994-01-07'  group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by bal",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplierBase where " +
      "l_shipdate >= " +
      "'1994-01-01'  and  l_shipdate " +
      "<= '1994-01-07' group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by bal"
  )

  cTest("gbexprtest2",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplier where l_shipdate >= " +
      "'1994-01-01'" +
      " and  l_shipdate <= '1994-01-07'  group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by o_orderdate, x, bal",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplierBase where To_date(l_shipdate) >= " +
      "cast('1994-01-01' as date) " +
      "and  To_date(l_shipdate) <= cast('1994-01-07' as date) group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 1, 10)) order by o_orderdate, x, bal"
  )

  cTest("gbexprtest3",
    "select o_orderdate, " +
      "(DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) as x " +
      "from orderLineItemPartSupplier  where l_shipdate >= '1994-01-01' " +
      "and  l_shipdate <= '1997-01-01' group by o_orderdate, " +
      "cast('2015-07-21 10:10:10 PST' as date), " +
      " (DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) " +
      "order by o_orderdate, x",
    "select o_orderdate, " +
      "(DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) as x " +
      "from orderLineItemPartSupplierBase where l_shipdate" +
      " >= '1994-01-01' " +
      "and  l_shipdate <= '1997-01-01' group by o_orderdate, " +
      "cast('2015-07-21 10:10:10 PST' as date), " +
      " (DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) " +
      "order by o_orderdate, x"
  )

  cTest("gbexprtest4",
    "select (Date_add(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  " +
      "and  l_shipdate  <= '1997-01-01'  group by " +
      "(Date_add(cast(o_orderdate as date), 360+3)) order by x",

    "select (Date_add(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate >= " +
      "'1994-01-01'  " +
      "and  l_shipdate <= '1997-01-01' group by  " +
      "(Date_add(cast(o_orderdate as date), 360+3)) order by x"
  )

  cTest("gbexprtest5",
    "select (Date_sub(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplier where l_shipdate >=" +
      "'1994-01-01' " +
      " and  l_shipdate <='1997-01-01'  group by " +
      "(Date_sub(cast(o_orderdate as date), 360+3)) order by x",
    "select (Date_sub(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplierBase where l_shipdate >= " +
      "'1994-01-01'  " +
      " and  l_shipdate <= '1997-01-01'  group by " +
      "(Date_sub(cast(o_orderdate as date), 360+3)) order by x"
  )

  cTest("gbexprtest6",
    "select o_orderdate, (weekofyear(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplier where l_shipdate >=  '1994-01-01'  and  " +
      "l_shipdate  <= '1997-01-01'  group by o_orderdate, " +
      "(weekofyear(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate",
    "select o_orderdate, (weekofyear(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplierBase where l_shipdate >=  '1994-01-01'  and " +
      " l_shipdate  <= '1997-01-01'  group by o_orderdate, " +
      "(weekofyear(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate"
  )

  cTest("gbexprtest7",
    "select o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplier where l_shipdate >=  '1994-01-01'  and " +
      " l_shipdate  <= '1997-01-01'  group by o_orderdate, " +
      "(unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate, x",
    "select o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplierBase where l_shipdate >=  '1994-01-01'  and  " +
      "l_shipdate  <= '1997-01-01'  group by o_orderdate, " +
      "(unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate, x"
  )


  cTest("gbexprtest8",
    "SELECT   o_orderdate, Cast(Concat(Year(Cast(o_orderdate AS TIMESTAMP)), " +
      "(CASE WHEN Month(Cast(o_orderdate AS TIMESTAMP))<4 " +
      "THEN '-01' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<7 " +
      "THEN '-04' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<10 " +
      "THEN '-07' ELSE '-10'  END), '-01 00:00:00') " +
      "AS TIMESTAMP) AS x " +
      "FROM (SELECT * FROM   orderLineItemPartSupplier) m " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "GROUP BY o_orderdate, cast(concat(year(cast(o_orderdate AS timestamp)), " +
      "(CASE WHEN month(cast(o_orderdate AS timestamp))<4 THEN '-01' " +
      "WHEN month(cast(o_orderdate AS timestamp))<7 THEN '-04' " +
      "WHEN month(cast(o_orderdate AS timestamp))<10 " +
      "THEN '-07' ELSE '-10' END), '-01 00:00:00') AS timestamp)" +
      " order by o_orderdate, x ",
    "SELECT   o_orderdate, Cast(Concat(Year(Cast(o_orderdate AS TIMESTAMP)), " +
      "(CASE WHEN Month(Cast(o_orderdate AS TIMESTAMP))<4 " +
      "THEN '-01' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<7 " +
      "THEN '-04' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<10 " +
      "THEN '-07' ELSE '-10'  END), '-01 00:00:00') " +
      "AS TIMESTAMP) AS x " +
      "FROM (SELECT * FROM   orderLineItemPartSupplierBase) m " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "GROUP BY o_orderdate, cast(concat(year(cast(o_orderdate AS timestamp)), " +
      "(CASE WHEN month(cast(o_orderdate AS timestamp))<4 THEN '-01' " +
      "WHEN month(cast(o_orderdate AS timestamp))<7 THEN '-04' " +
      "WHEN month(cast(o_orderdate AS timestamp))<10 " +
      "THEN '-07' ELSE '-10' END), '-01 00:00:00') AS timestamp)" +
      " order by o_orderdate, x "
  )

  cTest("gbexprtest9",
    "select o_orderdate as x " +
      "from orderLineItemPartSupplier " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "group by " +
      "o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) " +
      "order by o_orderdate, x",
    "select o_orderdate as x " +
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "group by " +
      "o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1)))  " +
      "order by o_orderdate, x"
  )

  /*
   * TODO: month function translation doesn't match Spark.

  cTest("gbexprtest10",
    " SELECT CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(o_orderdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplier) custom_sql_query " +
      "where l_shipdate >= '1994-01-01' and  " +
      "l_shipdate <= '1994-01-02'  " +
      "GROUP BY  " +
      "CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT), " +
      "YEAR(CAST(o_orderdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    " SELECT CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(o_orderdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplierBase) custom_sql_query " +
      "where l_shipdate >= '1994-01-01' and  " +
      "l_shipdate <= '1994-01-02'  " +
      "GROUP BY  " +
      "CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT), " +
      "YEAR(CAST(o_orderdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok"
  )
  */

  cTest("gbexprtest11",
    " SELECT CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(l_shipdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplier) custom_sql_query " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "GROUP BY  " +
      "CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT), " +
      "YEAR(CAST(l_shipdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    " SELECT CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(l_shipdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplierBase) custom_sql_query " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "GROUP BY  " +
      "CAST(((MONTH(CAST(l_shipdate AS TIMESTAMP)) - 1) / 3) * 2 AS BIGINT), " +
      "YEAR(CAST(l_shipdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok"
  )

  cTest("gbexprtest12",
    "select o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1)))) as x " +
      "from orderLineItemPartSupplier " +
      "where l_shipdate >= '1994-01-01' and  " +
      "l_shipdate <= '1997-01-01' " +
      "group by " +
      "o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1))))  " +
      "order by o_orderdate, x",
    "select o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1)))) as x " +
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate >= '1994-01-01' and  " +
      "l_shipdate <='1997-01-01' " +
      "group by " +
      "o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1))))  " +
      "order by o_orderdate, x"
  )

  cTest("gbexprtest13",
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
      "from orderLineItemPartSupplier " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate  <= '1997-01-01' " +
      "group by " +
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
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate >= '1994-01-01' and  " +
      "l_shipdate <= '1997-01-01'  " +
      "group by " +
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
      "order by o_orderdate, c1, c2, c3, c4"
  )


  cTest("gbexprtest14",
    "select o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20) as x " +
      "from orderLineItemPartSupplier " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "group by " +
      "o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20)" +
      "order by o_orderdate, x",
    "select o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20) as x " +
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "group by " +
      "o_orderdate, " +
      "date_add(cast(upper(concat(concat(substr(cast(cast(o_orderdate as timestamp) as string)," +
      " 1, 10), 't'), substr(cast(cast(o_orderdate as timestamp) as string), 12, 8))) as date)," +
      " 20)" +
      "order by o_orderdate, x"
  )

  cTest("gbexprtest15",
    "select o_orderdate, " +
      "Coalesce((CASE WHEN Month(Cast(o_orderdate AS date)) > 0  and " +
      "Month(Cast(o_orderdate AS date)) < 4 THEN \"Q1\" else null END), " +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) >= 4  and Month(Cast(o_orderdate AS date))" +
      " <= 6  THEN \"Q2\" else null END), (CASE WHEN Month(Cast(o_orderdate AS date)) > 6" +
      "  and Month(Cast(o_orderdate AS date)) <= 8  THEN \"Q3\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) <> 12    THEN \"Not Dec\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) = 12    THEN \"Dec\" else null END))as x " +
      "from orderLineItemPartSupplier " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01'  " +
      "group by " +
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
    "select o_orderdate, " +
      "Coalesce((CASE WHEN Month(Cast(o_orderdate AS date)) > 0  and " +
      "Month(Cast(o_orderdate AS date)) < 4 THEN \"Q1\" else null END), " +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) >= 4  and Month(Cast(o_orderdate AS date))" +
      " <= 6  THEN \"Q2\" else null END), (CASE WHEN Month(Cast(o_orderdate AS date)) > 6" +
      "  and Month(Cast(o_orderdate AS date)) <= 8  THEN \"Q3\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) <> 12    THEN \"Not Dec\" else null END)," +
      " (CASE WHEN Month(Cast(o_orderdate AS date)) = 12    THEN \"Dec\" else null END))as x " +
      "from orderLineItemPartSupplierBase " +
      "where l_shipdate >= '1994-01-01'  and  " +
      "l_shipdate <= '1997-01-01' " +
      "group by " +
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
      "order by o_orderdate, x"
  )


  cTest("aggTest1",
    """
      |SELECT MIN(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS x,
      |       MAX(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS y,
      |	  COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplier ) custom_sql_query
      | where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   HAVING (COUNT(1) > 0)
    """.stripMargin,
    """
      |SELECT MIN(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS x,
      |       MAX(CAST(CAST(l_shipdate AS TIMESTAMP) AS TIMESTAMP)) AS y,
      |	  COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplierBase ) custom_sql_query
      | where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   HAVING (COUNT(1) > 0)
    """.stripMargin
  )

  cTest("aggTest2",
    """
      |SELECT sum(l_quantity + 10) as s, MIN(l_quantity + 10) AS mi,
      |          MAX(l_quantity + 10) ma, COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplier ) custom_sql_query
      | where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1994-01-07'
      |   HAVING (COUNT(1) > 0) order by s, mi, ma
    """.stripMargin,
    """
      |SELECT sum(l_quantity + 10) as s, MIN(l_quantity + 10) AS mi,
      |          MAX(l_quantity + 10) ma, COUNT(1) AS c
      |   FROM ( select * from orderLineItemPartSupplierBase ) custom_sql_query
      | where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1994-01-07'
      |   HAVING (COUNT(1) > 0) order by s, mi, ma
    """.stripMargin
  )

  cTest("aggTest3",
    s"""
       |SELECT Min(Cast(Concat(To_date(Cast(Concat(To_date(l_shipdate),' 00:00:00')
       |AS TIMESTAMP)),' 00:00:00') AS TIMESTAMP)) AS mi,
       |max(cast(concat(to_date(cast(concat(to_date(l_shipdate),' 00:00:00') AS timestamp)),
       |' 00:00:00') AS timestamp)) AS ma,
       |count(1) as c
       |FROM   orderLineItemPartSupplier
       |where l_shipdate >= '1994-01-01'  and
       |   l_shipdate <= '1997-01-01'
       |HAVING (count(1) > 0)
     """.stripMargin,
    s"""
       |SELECT Min(Cast(Concat(To_date(Cast(Concat(To_date(l_shipdate),' 00:00:00')
       |AS TIMESTAMP)),' 00:00:00') AS TIMESTAMP)) AS mi,
       |max(cast(concat(to_date(cast(concat(to_date(l_shipdate),' 00:00:00') AS timestamp)),
       |' 00:00:00') AS timestamp)) AS ma,
       |count(1) as c
       |FROM   orderLineItemPartSupplierBase
       |where l_shipdate >= '1994-01-01'  and
       |   l_shipdate <= '1997-01-01'
       |HAVING (count(1) > 0)
     """.stripMargin
  )

  cTest("pmod1",
    """
      |SELECT max(pmod(o_totalprice, -5)) as s
      |   FROM orderLineItemPartSupplier
      | where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(pmod(o_totalprice, -5)) as s
      |   FROM orderLineItemPartSupplierBase
      |   where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )

  cTest("pmod2",
    """
      |SELECT max(pmod(-5, o_totalprice)) as s
      |   FROM orderLineItemPartSupplier
      |   where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(pmod(-5, o_totalprice)) as s
      |   FROM orderLineItemPartSupplierBase
      |   where l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )

  cTest("startsWith",
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplier
      |   where c_name like 'C%' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplierBase
      |   where c_name like 'C%' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )

  cTest("endsWith",
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplier
      |   where c_name like '%7' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplierBase
      |   where c_name like '%7' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )

  cTest("contains",
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplier
      |   where c_name like '%7%' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplierBase
      |   where c_name like '%7%' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )

  cTest("like",
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplier
      |   where c_name like 'C%7' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplierBase
      |   where c_name like 'C%7' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )

  cTest("rlike",
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplier
      |   where c_name like '#.*7' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin,
    """
      |SELECT max(o_totalprice) as s
      |   FROM orderLineItemPartSupplierBase
      |   where c_name like '#.*7' and
      |   l_shipdate >= '1994-01-01'  and
      |   l_shipdate <= '1997-01-01'
      |   group by s_region
      |   order by s
    """.stripMargin
  )


  cTest("strGTLTEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where (o_orderdate <= '1993-12-12') and
      |(o_orderdate >= '1993-10-12')
      |and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin,
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where (o_orderdate <= '1993-12-12') and
      |(o_orderdate >= '1993-10-12')
      |and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
  )

  cTest("dateEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where cast(o_orderdate as date) = cast('1994-06-30' as Date)
      |and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin,
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where cast(o_orderdate as date) = cast('1994-06-30' as Date)
      |and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin
  )

  cTest("dateGTLTEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where (cast(o_orderdate as date) <= cast('1993-12-12' as Date))
      | and (cast(o_orderdate as date) >= cast('1993-10-12' as Date))
      | and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin,
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where (cast(o_orderdate as date) <= cast('1993-12-12' as Date))
      | and (cast(o_orderdate as date) >= cast('1993-10-12' as Date))
      | and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin)

  cTest("tsGTLTEq1",
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplier
      |where (cast(l_shipdate as timestamp) <= cast('1993-12-12 00:00:00' as timestamp))
      | and (cast(l_shipdate as timestamp) >= cast('1993-10-12 00:00:00' as timestamp))
      | and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin,
    """
      |select o_orderstatus as x, cast(o_orderdate as date) as y
      |from orderLineItemPartSupplierBase
      |where (cast(l_shipdate as timestamp) <= cast('1993-12-12 00:00:00' as timestamp))
      | and (cast(l_shipdate as timestamp) >= cast('1993-10-12 00:00:00' as timestamp))
      | and (l_shipdate >= '1994-01-01'  and l_shipdate <= '1997-01-01')
      |group by o_orderstatus, cast(o_orderdate as date)
      |order by x, y
    """.stripMargin)


  cTest("inclause-insetTest1",
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplier
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate)
      <= cast('1997-12-31' as date)
      and cast(order_year as int) in (1985,1986,1987,1988,1989,1990,1991,1992,
      1993,1994,1995,1996,1997,1998,1999,2000, null) and (l_shipdate >= '1994-01-01'
      and l_shipdate <= '1994-01-02')
      group by c_name
      order by c_name, bal""".stripMargin,
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplierBase
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate)
      <= cast('1997-12-31' as date)
      and cast(order_year as int) in (1985,1986,1987,1988,1989,1990,1991,1992,
      1993,1994,1995,1996,1997,1998,1999,2000, null) and (l_shipdate >= '1994-01-01'
      and l_shipdate <= '1994-01-02')
      group by c_name
      order by c_name, bal""".stripMargin
  )

  cTest("inclause-inTest1",
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplier
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate)
      <= cast('1997-12-31' as date)
       and cast(order_year as int) in (1993,1994,1995, null) and (l_shipdate >= '1994-01-01'
       and l_shipdate <= '1994-01-02')
      group by c_name
      order by c_name, bal""".stripMargin,
    s"""select c_name, sum(c_acctbal) as bal
      from orderLineItemPartSupplierBase
      where to_Date(o_orderdate) >= cast('1993-01-01' as date) and to_Date(o_orderdate)
      <= cast('1997-12-31' as date)
       and cast(order_year as int) in (1993,1994,1995, null) and (l_shipdate >= '1994-01-01'
       and l_shipdate <= '1994-01-02')
      group by c_name
      order by c_name, bal""".stripMargin
  )


}
