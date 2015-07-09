package org.sparklinedata.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{TableScan, BaseRelation}
import org.apache.spark.sql.types.StructType

class DruidRelation protected[druid] ()(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
  /*
   pass in
   - connection info to druid (host,port, dataSource, params)
   - sourceDF
   - a mapping from srcDF columns to Druid dims + metrics
   - optionally a  Druid Query
   */

  override def schema: StructType = ???

  override def buildScan(): RDD[Row] = ???
}
