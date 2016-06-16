package org.sparklinedata.druid.client

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll


class DataSourceCTest extends BaseTest with BeforeAndAfterAll with Logging{

  cTest("datasourceT1",
    "select * from orderLineItemPartSupplier where l_shipdate  >= '1994-01-01' " +
      " and l_shipdate <= '1997-01-01' "
    ,
    "select * from orderLineItemPartSupplierBase where l_shipdate  >= '1994-01-01' " +
      "and l_shipdate <= '1997-01-01' "
  )
}
