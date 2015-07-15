package org.sparklinedata.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{TableScan, BaseRelation}
import org.apache.spark.sql.types.{StructField, StructType}
import org.joda.time.Interval
import org.sparklinedata.druid.metadata.{DruidDataType, DruidRelationInfo}

case class DruidQuery(q : QuerySpec, intervalSplits : List[Interval]) {

  def this(q : QuerySpec) = this(q, q.intervals.map(Interval.parse(_)))

  def schema(dInfo : DruidRelationInfo) : StructType = {

    val fields : List[StructField] = q.dimensions.map{d =>
      new StructField(d.outputName, d.sparkDataType(dInfo.druidDS))
    } ++
    q.aggregations.map {a =>
      new StructField(a.name, a.sparkDataType(dInfo.druidDS))
    } ++
    q.postAggregations.map{ ps =>
      ps.map {p =>
        new StructField(p.name, p.sparkDataType(dInfo.druidDS))
      }
    }.getOrElse(Nil)

    StructType(fields)
  }
}

case class DruidRelation protected[druid] (val info : DruidRelationInfo,
                                       val dQuery : Option[DruidQuery])(
  @transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
  /*
   pass in
   - connection info to druid (host,port, dataSource, params)
   - sourceDF
   - a mapping from srcDF columns to Druid dims + metrics
   - optionally a  Druid Query
   */

  override def schema: StructType =
    dQuery.map(_.schema(info)).getOrElse(info.sourceDF(sqlContext).schema)

  override def buildScan(): RDD[Row] =
    dQuery.map(new DruidRDD(sqlContext, info, _)).getOrElse(info.sourceDF(sqlContext).rdd)
}
