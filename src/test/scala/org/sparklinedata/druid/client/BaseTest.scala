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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.scalatest.{ BeforeAndAfterAll, fixture, TestData }
import org.sparklinedata.spark.dateTime.Functions._
import scala.io.Source
import org.scalatest.fixture.TestDataFixture
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import java.util.UUID
import org.apache.spark.sql.types._
//import ScalaReflection._
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import scala.language.implicitConversions


abstract class AbstractTreeNode[BaseType <: AbstractTreeNode[BaseType]] extends TreeNode[BaseType] with Logging {
  self: BaseType =>
    
  protected def jsonFields: List[JField] = {
    
    val fieldValues = productIterator.toSeq ++ otherCopyArgs
    val fieldNames =  Seq.fill(fieldValues.length)("sparkline")//getConstructorParameterNames(getClass) 
    
    logInfo("FieldValues\n" + fieldValues.toString)
    assert(fieldNames.length == fieldValues.length, s"${getClass.getSimpleName} fields: " +
      fieldNames.mkString(", ") + s", values: " + fieldValues.map(_.toString).mkString(", "))
    fieldNames.zip(fieldValues).map {
      // If the field value is a child, then use an int to encode it, represents the index of
      // this child in all children.
      case (name, value: TreeNode[_]) if containsChild(value) =>
        name -> JInt(children.indexOf(value))
      case (name, value: Seq[BaseType]) if value.toSet.subsetOf(containsChild) =>
        name -> JArray(
          value.map(v => JInt(children.indexOf(v.asInstanceOf[TreeNode[_]]))).toList)
      case (name, value) => name -> parseToJson(value)
    }.toList
  }

  def toJSON: String = compact(render(jsonValue))

  def prettyJson: String = pretty(render(jsonValue))

  private def jsonValue: JValue = {
    val jsonValues = scala.collection.mutable.ArrayBuffer.empty[JValue]
    def collectJsonValue(tn: BaseType): Unit = {
      val jsonFields = ("class" -> JString(tn.getClass.getName)) ::
        ("num-children" -> JInt(tn.children.length)) :: tn.jsonFields
      jsonValues += JObject(jsonFields)
      tn.children.foreach(collectJsonValue)
    }
    collectJsonValue(this)
    jsonValues
  }

  private def parseToJson(obj: Any): JValue = obj match {
    case b: Boolean   => JBool(b)
    case b: Byte      => JInt(b.toInt)
    case s: Short     => JInt(s.toInt)
    case i: Int       => JInt(i)
    case l: Long      => JInt(l)
    case f: Float     => JDouble(f)
    case d: Double    => JDouble(d)
    case b: BigInt    => JInt(b)
    case null         => JNull
    case s: String    => JString(s)
    case u: UUID      => JString(u.toString)
    case dt: DataType => dt.typeName
    //case m: Metadata  => m.jsonValue
    case s: StorageLevel =>
      ("useDisk" -> s.useDisk) ~ ("useMemory" -> s.useMemory) ~ ("useOffHeap" -> s.useOffHeap) ~
        ("deserialized" -> s.deserialized) ~ ("replication" -> s.replication)
    case n: AbstractTreeNode[_] => n.jsonValue
    case o: Option[_]           => o.map(parseToJson)
    case t: Seq[_]              => JArray(t.map(parseToJson).toList)
    case m: Map[_, _] =>
      val fields = m.toList.map { case (k: String, v) => (k, parseToJson(v)) }
      JObject(fields)
    case r: RDD[_]                                 => JNothing
    // if it's a scala object, we can simply keep the full class path.
    // TODO: currently if the class name ends with "$", we think it's a scala object, there is
    // probably a better way to check it.
    case obj if obj.getClass.getName.endsWith("$") => "object" -> obj.getClass.getName
    // returns null if the product type doesn't have a primary constructor, e.g. HiveFunctionWrapper
    case p: Product => try {
      
      val fieldValues = p.productIterator.toSeq
      val fieldNames = Seq.fill(fieldValues.length)("") //getConstructorParameterNames(p.getClass) 
      assert(fieldNames.length == fieldValues.length)
      ("product-class" -> JString(p.getClass.getName)) :: fieldNames.zip(fieldValues).map {
        case (name, value) => name -> parseToJson(value)
      }.toList
    } catch {
      case _: RuntimeException => null
    }
    case _ => JNull
  }
}

class Plan(lp: List[LogicalPlan]) extends AbstractTreeNode[Plan]{
    
  override def children = Seq.empty
    
  
  def canEqual(that: Any): Boolean = lp.canEqual(that)

  def productArity: Int = lp.length 

  def productElement(n: Int): Any = lp(n)
  
}

abstract class BaseTest extends fixture.FunSuite with BeforeAndAfterAll with TestDataFixture with Logging {

  val colMapping =
    """{
      | "l_quantity" : "sum_l_quantity",
      | "ps_availqty" : "sum_ps_availqty",
      | "cn_name" : "c_nation",
      | "cr_name" : "c_region",
      |  "sn_name" : "s_nation",
      | "sr_name" : "s_region"
      |}
    """.stripMargin.replace('\n', ' ')

  val functionalDependencies =
    """[
      |  {"col1" : "c_name", "col2" : "c_address", "type" : "1-1"},
      |  {"col1" : "c_phone", "col2" : "c_address", "type" : "1-1"},
      |  {"col1" : "c_name", "col2" : "c_mktsegment", "type" : "n-1"},
      |  {"col1" : "c_name", "col2" : "c_comment", "type" : "1-1"},
      |  {"col1" : "c_name", "col2" : "c_nation", "type" : "n-1"},
      |  {"col1" : "c_nation", "col2" : "c_region", "type" : "n-1"}
      |]
    """.stripMargin.replace('\n', ' ')

  val flatStarSchema =
    """
      |{
      |  "factTable" : "orderLineItemPartSupplier",
      |  "relations" : []
      | }
    """.stripMargin.replace('\n', ' ')

  val starSchema =
    """
    |{
    |  "factTable" : "lineitem",
    |  "relations" : [ {
    |    "leftTable" : "lineitem",
    |    "rightTable" : "orders",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "l_orderkey",
    |      "rightAttribute" : "o_orderkey"
    |    } ]
    |  }, {
    |    "leftTable" : "lineitem",
    |    "rightTable" : "partsupp",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "l_partkey",
    |      "rightAttribute" : "ps_partkey"
    |    }, {
    |      "leftAttribute" : "l_suppkey",
    |      "rightAttribute" : "ps_suppkey"
    |    } ]
    |  }, {
    |    "leftTable" : "partsupp",
    |    "rightTable" : "part",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "ps_partkey",
    |      "rightAttribute" : "p_partkey"
    |    } ]
    |  }, {
    |    "leftTable" : "partsupp",
    |    "rightTable" : "supplier",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "ps_suppkey",
    |      "rightAttribute" : "s_suppkey"
    |    } ]
    |  }, {
    |    "leftTable" : "orders",
    |    "rightTable" : "customer",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "o_custkey",
    |      "rightAttribute" : "c_custkey"
    |    } ]
    |  }, {
    |    "leftTable" : "customer",
    |    "rightTable" : "custnation",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "c_nationkey",
    |      "rightAttribute" : "cn_nationkey"
    |    } ]
    |  }, {
    |    "leftTable" : "custnation",
    |    "rightTable" : "custregion",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "cn_regionkey",
    |      "rightAttribute" : "cr_regionkey"
    |    } ]
    |  }, {
    |    "leftTable" : "supplier",
    |    "rightTable" : "suppnation",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "s_nationkey",
    |      "rightAttribute" : "sn_nationkey"
    |    } ]
    |  }, {
    |    "leftTable" : "suppnation",
    |    "rightTable" : "suppregion",
    |    "relationType" : "n-1",
    |    "joinCondition" : [ {
    |      "leftAttribute" : "sn_regionkey",
    |      "rightAttribute" : "sr_regionkey"
    |    } ]
    |  } ]
    |}
  """.stripMargin.replace('\n', ' ')

  override def beforeAll() = {

    register(TestHive)
    DruidPlanner(TestHive)

    val cT = s"""CREATE TABLE if not exists orderLineItemPartSupplierBase(o_orderkey integer,
             o_custkey integer,
      o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string,
      o_clerk string,
      o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer,
      l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double,
      l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
      l_shipinstruct string,
      l_shipmode string, l_comment string, order_year string, ps_partkey integer,
      ps_suppkey integer,
      ps_availqty integer, ps_supplycost double, ps_comment string, s_name string, s_address string,
      s_phone string, s_acctbal double, s_comment string, s_nation string,
      s_region string, p_name string,
      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
      p_retailprice double,
      p_comment string, c_name string , c_address string , c_phone string , c_acctbal double ,
      c_mktsegment string , c_comment string , c_nation string , c_region string)
      USING com.databricks.spark.csv
      OPTIONS (path "/Users/hbutani/tpch/datascale1/orderLineItemPartSupplierCustomer.small/",
      header "false", delimiter "|")""".stripMargin

    println(cT)
    sql(cT)

    TestHive.table("orderLineItemPartSupplierBase").cache()

    // sql("select * from orderLineItemPartSupplierBase limit 10").show(10)

    val cTOlap = s"""CREATE TABLE if not exists orderLineItemPartSupplier
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      druidPort "8082",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchema')""".stripMargin

    println(cTOlap)
    sql(cTOlap)
  }

  def sqlAndLog(nm: String, sqlStr: String): DataFrame = {
    logInfo(s"\n$nm SQL:\n" + sqlStr)
    sql(sqlStr)
  }

  def logPlan(nm: String, df: DataFrame): Unit = {
    logInfo(s"\n$nm Plan:")
    //logInfo(s"\nLogical Plan:\n" + df.queryExecution.optimizedPlan.toString)
    //logInfo(s"\nPhysical Plan:\n" + df.queryExecution.sparkPlan.toString)
  }

  def turnOnTransformDebugging: Unit = {
    TestHive.setConf(DruidPlanner.DEBUG_TRANSFORMATIONS.key, "true")
  }

  def turnOffTransformDebugging: Unit = {
    TestHive.setConf(DruidPlanner.DEBUG_TRANSFORMATIONS.key, "false")
  }

  val LOGICAL_PLAN_EXT = ".logicalplan"
  val PHYSICAL_PLAN_EXT = ".physicalplan"

  def removeTags(plan: String): String = {
    val cleaned = plan.replaceAll("#\\d+|@\\w+", "")
    cleaned.trim
  }

  def readFileContents(fileName: String): String = {
    Source.fromURL(getClass.getResource("/" + fileName)).getLines().mkString("\n")
  }

  //implicit def plan2Json(logicalPlan: LogicalPlan) = new Plan(logicalPlan.children.toList)

  def compareLogicalPlan(df: DataFrame, td: TestData): Boolean = {

    //Get the calling class name. `this` will be bound for the concrete class  
    val goldenFileName = this.getClass().getSimpleName + "_" + td.name + LOGICAL_PLAN_EXT

    val goldenFileContents = readFileContents(goldenFileName)
    val logicalPlan = df.queryExecution.optimizedPlan.toString
    logInfo("Children \n" + df.queryExecution.optimizedPlan.children.toList)
    
    val p = new Plan(df.queryExecution.optimizedPlan.children.toList)
    p.children
    logInfo("Converting Logical to Json\n" + p.toJSON)
    
   //logInfo("Json DUMP \n" + df.queryExecution.optimizedPlan.toJSON)

    val cleanedGolden = removeTags(goldenFileContents)
    val cleanedLogical = removeTags(logicalPlan)

    cleanedGolden == cleanedLogical

  }

  def comparePhysicalPlan(df: DataFrame, td: TestData): Boolean = {
    //Get the calling class name. `this` will be bound for the concrete class
    val goldenFileName = this.getClass().getSimpleName + "_" + td.name + PHYSICAL_PLAN_EXT

    val goldenFileContents = readFileContents(goldenFileName)
    val physicalPlan = df.queryExecution.sparkPlan.toString()
    
    val cleanedGolden = removeTags(goldenFileContents)
    val cleanedPhysical = removeTags(physicalPlan)

    cleanedGolden == cleanedPhysical
  }

}

