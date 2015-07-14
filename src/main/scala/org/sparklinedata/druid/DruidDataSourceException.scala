package org.sparklinedata.druid

class DruidDataSourceException(message: String, cause: Throwable)
  extends Exception(message, cause) {
  def this(message : String) = this(message, null)
}
