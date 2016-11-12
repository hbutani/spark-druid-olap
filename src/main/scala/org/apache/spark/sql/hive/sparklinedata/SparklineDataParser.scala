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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.sparklinedata.commands.{ClearMetadata, CreateStarSchema, ExplainDruidRewrite}
import org.apache.spark.sql.util.PlanUtil
import org.sparklinedata.druid.metadata.{EqualityCondition, FunctionalDependencyType, StarRelationInfo, StarSchemaInfo}

class SPLParser(sparkSession: SparkSession,
                baseParser : ParserInterface,
                moduleParserExtensions : Seq[SparklineDataParser] = Nil,
                moduleParserTransforms : Seq[RuleExecutor[LogicalPlan]] = Nil
                          ) extends ParserInterface {
  val parsers = {
    if (moduleParserExtensions.isEmpty) {
      Seq(new SparklineDruidCommandsParser(sparkSession))
    } else {
      moduleParserExtensions
    }
  }

  override def parsePlan(sqlText: String): LogicalPlan = {

    val r : SparklineDataParser#ParseResult[LogicalPlan] = null

    val splParsedPlan = parsers.foldRight(r) {
      case (p, o) if o == null || !o.successful => p.parse2(sqlText)
      case (_, o)  => o
    }

    val parsedPlan = if (splParsedPlan.successful ) {
      splParsedPlan.get
    } else {
      try {
        baseParser.parsePlan(sqlText)
      } catch {
        case pe : ParseException => {
          val splFailureDetails = splParsedPlan.asInstanceOf[SparklineDataParser#NoSuccess].msg
          throw new ParseException(pe.command,
            pe.message + s"\nSPL parse attempt message: $splFailureDetails",
            pe.start,
            pe.stop
          )
        }
      }
    }

    moduleParserTransforms.foldRight(parsedPlan){
      case (rE, lP) => rE.execute(lP)
    }
  }

  def parseExpression(sqlText: String): Expression =
    baseParser.parseExpression(sqlText)

  def parseTableIdentifier(sqlText: String): TableIdentifier =
  baseParser.parseTableIdentifier(sqlText)
}

abstract class SparklineDataParser extends AbstractSparkSQLParser {

  def parse2(input: String): ParseResult[LogicalPlan]
}

class SparklineDruidCommandsParser(sparkSession: SparkSession) extends SparklineDataParser {

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
  protected val ONE_TO_ONE = Keyword("ONE_TO_ONE")
  protected val MANY_TO_ONE = Keyword("MANY_TO_ONE")
  protected val JOIN = Keyword("JOIN")
  protected val OF = Keyword("OF")
  protected val WITH = Keyword("WITH")
  protected val AND = Keyword("AND")
  protected val CREATE = Keyword("CREATE")
  protected val ALTER = Keyword("ALTER")
  protected val STAR = Keyword("STAR")
  protected val SCHEMA = Keyword("SCHEMA")
  protected val AS = Keyword("AS")

  def parse2(input: String): ParseResult[LogicalPlan] = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input))
  }

  protected override lazy val start: Parser[LogicalPlan] =
    clearDruidCache | execDruidQuery | explainDruidRewrite | starSchema

  protected lazy val clearDruidCache: Parser[LogicalPlan] =
    CLEAR ~> DRUID ~> CACHE ~> opt(ident) ^^ {
      case id => ClearMetadata(id)
    }

  protected lazy val execDruidQuery : Parser[LogicalPlan] =
    (ON ~> DRUIDDATASOURCE ~> ident) ~ (USING ~> HISTORICAL).? ~
      (EXECUTE ~> opt(QUERY) ~> restInput) ^^ {
      case ds ~ hs ~ query => {
        PlanUtil.logicalPlan(ds, query, hs.isDefined)(sparkSession.sqlContext)
      }
    }

  protected lazy val explainDruidRewrite: Parser[LogicalPlan] =
    EXPLAIN ~> DRUID ~> REWRITE ~> restInput ^^ {
      case sql => ExplainDruidRewrite(sql)
    }

  private lazy val starRelationType : Parser[FunctionalDependencyType.Value] =
    (ONE_TO_ONE | MANY_TO_ONE) ^^ {
      case s if s == ONE_TO_ONE.normalize => FunctionalDependencyType.OneToOne
      case s if s == MANY_TO_ONE.normalize => FunctionalDependencyType.ManyToOne
    }

  private lazy val joinEqualityCond : Parser[EqualityCondition] =
    (qualifiedId <~ "=") ~ qualifiedId ^^ {
      case ~(l,r) => EqualityCondition(l,r)
    }

  private lazy val starRelation : Parser[StarRelationInfo] =
    starRelationType.? ~ JOIN ~ OF ~ qualifiedId ~
      WITH ~ qualifiedId ~ ON ~ repsep(joinEqualityCond, AND) ^^ {
      case srI ~ _ ~ _ ~ leftTable ~ _ ~ rightTable ~ _ ~ joinEqConds => {
        StarRelationInfo(
          leftTable,
          rightTable,
          srI.getOrElse(FunctionalDependencyType.ManyToOne),
          joinEqConds
        )
      }
    }

  private lazy val starSchema : Parser[LogicalPlan] =
    createOrAlter ~ STAR ~ SCHEMA ~ ON ~ qualifiedId ~ opt(AS ~ repsep(starRelation, opt(","))) ^^ {
      case c ~ _ ~ _ ~ _ ~ factTable ~ Some(_ ~ starRelations)  =>
        CreateStarSchema(StarSchemaInfo(factTable, starRelations:_*), !c)
      case c ~ _ ~ _ ~ _ ~ factTable ~ None  => CreateStarSchema(StarSchemaInfo(factTable), !c)
    }

  private lazy val createOrAlter : Parser[Boolean] =
    (CREATE | ALTER ) ^^ {
      case s if s == CREATE.normalize => true
      case _ => false
    }

  private lazy val qualifiedId : Parser[String] =
    (ident ~ ("." ~> ident).?) ^^ {
      case ~(n, None) => n
      case ~(q, Some(n)) => s"$q.$n"
    }

}