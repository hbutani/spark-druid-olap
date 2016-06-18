package org.sparklinedata.druid.client

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll

import scala.language.postfixOps
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

class FilterCTest extends BaseTest with BeforeAndAfterAll with Logging{
  cTest("filterT1",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment in ('MACHINERY', 'HOUSEHOLD') and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by c_name",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplierBase " +
      "where c_mktsegment in ('MACHINERY', 'HOUSEHOLD') and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by c_name"
  )

  cTest("filterT2",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where l_linenumber in (1, 2, 3, 4, 5, 6, 7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22," +
      "23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45) and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by c_name",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplierBase " +
      "where l_linenumber in (1, 2, 3, 4, 5, 6, 7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22," +
      "23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45) and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by c_name"
  )

  cTest("filterT3",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment not in ('MACHINERY', 'HOUSEHOLD') and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by c_name",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplierBase " +
      "where c_mktsegment not in ('MACHINERY', 'HOUSEHOLD') and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by c_name"
  )

  cTest("filterT4",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplier " +
      "where c_mktsegment !=  'MACHINERY' and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by c_name",
    "select c_name, sum(c_acctbal) as bal " +
      "from orderLineItemPartSupplierBase " +
      "where c_mktsegment !=  'MACHINERY' and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' " +
      "group by c_name"
  )

  cTest("filterT5",
    "select s_region from orderLineItemPartSupplier" +
      " where l_shipdate is not null and l_discount is not null and p_name is not null and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      " group by s_region " +
      " order by s_region",
    "select s_region from orderLineItemPartSupplierBase" +
      " where l_shipdate is not null and l_discount is not null and p_name is not null and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      " group by s_region " +
      " order by s_region"
  )

  cTest("filterT6",
    "select s_region from orderLineItemPartSupplier" +
      " where (cast(l_shipdate as double) + 10) is not null and " +
      " (l_discount % 10) is not null and p_name  is not null  and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      " group by s_region " +
      " order by s_region",
    "select s_region from orderLineItemPartSupplierBase" +
      " where (cast(l_shipdate as double) + 10) is not null and " +
      " (l_discount % 10) is not null and p_name  is not null  and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      " group by s_region " +
      " order by s_region"
  )


  cTest("filterT7",
    "select s_region from orderLineItemPartSupplier" +
      " where not(l_shipdate is null) and not(l_discount is null) and not(p_name is null) and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      " group by s_region " +
      " order by s_region",
    "select s_region from orderLineItemPartSupplierBase" +
      " where not(l_shipdate is null) and not(l_discount is null) and not(p_name is null) and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      " group by s_region " +
      " order by s_region"
  )



  cTest("filterT8",
    "select s_region from orderLineItemPartSupplier" +
      " where not((cast(l_shipdate as double) + 10) is null) and  l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' and " +
      " not((l_discount % 10) is null) and not(p_name  is null)" +
      " group by s_region " +
      " order by s_region",
    "select s_region from orderLineItemPartSupplierBase" +
      " where not((cast(l_shipdate as double) + 10) is null) and l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01' and " +
      " not((l_discount % 10) is null) and not(p_name  is null)" +
      " group by s_region " +
      " order by s_region"
  )
}
