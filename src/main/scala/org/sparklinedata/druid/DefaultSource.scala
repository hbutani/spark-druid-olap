package org.sparklinedata.druid

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class DefaultSource extends RelationProvider {

  private def checkSourceDF(sqlContext: SQLContext, parameters: Map[String, String]): DataFrame = {
    val tabName =
      parameters.getOrElse("sourceDF", sys.error("'sourceDF' must be specified for Druid OLAP."))
    sqlContext.table(tabName)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val sourceDF = checkSourceDF(sqlContext, parameters)

  ???
  }
}
