package org.sparklinedata.druid.client

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll


class DruidReWriteOrderCTest extends BaseTest with BeforeAndAfterAll with Logging {
  cTest("druidRewriteOrderT1",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_linestatus"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_linestatus"
  )
  cTest("druidRewriteOrderT2",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_returnflag " +
      "limit 2"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a," +
      "count(distinct o_orderkey)  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by l_returnflag " +
      "limit 2"
  )
  cTest("druidRewriteOrderT3",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by count(*)"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by count(*)"
  )
  cTest("druidRewriteOrderT4",
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by s"
    ,
    "select l_returnflag, l_linestatus, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by s"
  )
  cTest("druidRewriteOrderT5",
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by s, ls, r " +
      "limit 3"
    ,
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by s, ls, r " +
      "limit 3"
  )
  cTest("druidRewriteOrderT6",
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by m desc, s, r " +
      "limit 3"
    ,
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*), sum(l_extendedprice) as s, max(ps_supplycost) as m, avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by m desc, s, r " +
      "limit 3"
  )
  cTest("druidRewriteOrderT7",
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, " +
      "avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by c " +
      "limit 3"
    ,
    "select l_returnflag as r, l_linestatus as ls, " +
      "count(*) + 1 as c, sum(l_extendedprice) as s, max(ps_supplycost) as m, " +
      "avg(ps_availqty) as a  " +
      "from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01'  and l_shipdate <= '1997-01-01'" +
      "group by l_returnflag, l_linestatus " +
      "order by c " +
      "limit 3"
  )
}
