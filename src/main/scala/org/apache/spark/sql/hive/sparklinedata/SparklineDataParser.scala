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

package org.apache.spark.sql.hive.sparklinedata

import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.{HiveContext, HiveQLDialect, HiveQl}
import org.apache.spark.sql.sparklinedata.commands.{ClearMetadata, ExplainDruidRewrite}
import org.apache.spark.sql.util.PlanUtil

class SparklineDataDialect(
                            sqlContext: HiveContext,
                            moduleParserExtensions : Seq[SparklineDataParser] = Nil
                          ) extends HiveQLDialect(sqlContext) {
  val parsers = {
    if (moduleParserExtensions.isEmpty) {
      Seq(new SparklineDruidCommandsParser(sqlContext))
    } else {
      moduleParserExtensions
    }
  }

  override def parse(sqlText: String): LogicalPlan = {
    sqlContext.executionHive.withHiveState {

      parsers.foldLeft(None:Option[LogicalPlan]) {
        case (None, p) => p.parse2(sqlText)
        case (Some(lP), _) => Some(lP)
      }.getOrElse(HiveQl.parseSql(sqlText))
    }
  }
}

abstract class SparklineDataParser extends AbstractSparkSQLParser {
  def parse2(input: String): Option[LogicalPlan]
}

class SparklineDruidCommandsParser(sqlContext: HiveContext) extends SparklineDataParser {

  protected val CLEAR = Keyword("CLEAR")
  protected val DRUID = Keyword("DRUID")
  protected val CACHE = Keyword("CACHE")
  protected val DRUIDDATASOURCE = Keyword("DRUIDDATASOURCE")
  protected val ON = Keyword("ON")
  protected val EXECUTE = Keyword("EXECUTE")
  protected val QUERY = Keyword("QUERY")
  protected val USING = Keyword("USING")
  protected val HISTORICAL = Keyword("HISTORICAL")
  protected val EXPLAIN = Keyword("EXPLAIN")
  protected val REWRITE = Keyword("REWRITE")

  def parse2(input: String): Option[LogicalPlan] = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => Some(plan)
      case failureOrError => None
    }
  }

  protected override lazy val start: Parser[LogicalPlan] =
    clearDruidCache | execDruidQuery | explainDruidRewrite

  protected lazy val clearDruidCache: Parser[LogicalPlan] =
    CLEAR ~> DRUID ~> CACHE ~> opt(ident) ^^ {
      case id => ClearMetadata(id)
    }

  protected lazy val execDruidQuery : Parser[LogicalPlan] =
    (ON ~> DRUIDDATASOURCE ~> ident) ~ (USING ~> HISTORICAL).? ~
      (EXECUTE ~> opt(QUERY) ~> restInput) ^^ {
      case ds ~ hs ~ query => {
        PlanUtil.logicalPlan(ds, query, hs.isDefined)(sqlContext)
      }
    }

  protected lazy val explainDruidRewrite: Parser[LogicalPlan] =
    EXPLAIN ~> DRUID ~> REWRITE ~> restInput ^^ {
      case sql => ExplainDruidRewrite(sql)
    }

}