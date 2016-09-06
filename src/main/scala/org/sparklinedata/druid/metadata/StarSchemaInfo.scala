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

package org.sparklinedata.druid.metadata

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.hive.sparklinedata.SparklineDataContext

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * Describes the relations in a Star Schema. Used to build a [[StarSchema]] model.
 * This will be part of the [[org.sparklinedata.druid.DefaultSource DruidDataSource]] defintion.
 *
 * @param factTable name of the fact table for the StarSchema
 * @param relations how are tables related in this StarSchema.
 */
case class StarSchemaInfo(factTable : String, relations : StarRelationInfo*)

object StarSchemaInfo {

  def qualifyTableNames(sqlContext : SQLContext,
                        sSI : StarSchemaInfo) : StarSchemaInfo = {
    StarSchemaInfo(
      SparklineDataContext.qualifiedName(sqlContext, sSI.factTable),
      sSI.relations.map(StarRelationInfo.qualifyTableNames(sqlContext, _)):_*
    )
  }
}

/**
 * Represents how 2 tables in a StarSchema are related.
 * @param leftTable
 * @param rightTable
 * @param relationType is it a 1-1 or n-1 relation. As the relation Graph is traversed outward from
 *                     the fact table, only these relations are supported. The implication is we
 *                     don't support fact-to-fact relations or relation between dimensions.
 * @param joinCondition
 */
case class StarRelationInfo(leftTable : String,
                             rightTable : String,
                             relationType : FunctionalDependencyType.Value,
                              joinCondition : Seq[EqualityCondition]) {

}

object StarRelationInfo {

  def oneToone(leftTable : String,
           rightTable : String,
           joinCondition : (String,String)* ) : StarRelationInfo =
    new StarRelationInfo(leftTable, rightTable, FunctionalDependencyType.OneToOne,
      joinCondition.map(t => EqualityCondition(t._1, t._2)))

  def manyToone(leftTable : String,
               rightTable : String,
               joinCondition : (String,String)* ) : StarRelationInfo =
    new StarRelationInfo(leftTable, rightTable, FunctionalDependencyType.ManyToOne,
      joinCondition.map(t => EqualityCondition(t._1, t._2)))


  def qualifyTableNames(sqlContext : SQLContext,
                        sRI : StarRelationInfo) : StarRelationInfo = {
    sRI.copy(
      leftTable = SparklineDataContext.qualifiedName(sqlContext, sRI.leftTable),
      rightTable = SparklineDataContext.qualifiedName(sqlContext, sRI.rightTable)
    )
  }

}

case class EqualityCondition(leftAttribute : String, rightAttribute : String)

case class StarRelation(tableName : String,
                             relationType : FunctionalDependencyType.Value,
                             joiningKeys : Set[(String, String)])

/**
 * Represents a Table in the StarSchema.
 *
 * @param name
 * @param parent Its parent table along the join path from this Table to the Fact table
 *               of the Star Schema.
 */
case class StarTable(name : String,
                      parent : Option[StarRelation])

/**
 * Represents a StarSchema. The '''Star Schema'''s we support have the following __constraints__:
 *  - We only support '''one-one''' or '''many-one''' relations between entities.
 *  - A table can be related to the '''Fact Table''' via only 1 unique Path.
 *  - The ''column names'' across the Star Schema must be unique. So 2 tables in the Star Schema
 *  cannot have columns with the same name.
 *
 *  The first 2 points are not an issue only in the most involved star schema models; for e.g.
 *  we show how tpch can be modeled below. The 3rd restriction is an implementation issue:
 *  when performing QueryPlan rewrites we don't have access to the table an Attribute belongs to,
 *  for now we get around this issue by forcing column names to be unique across the Star Schema.
 *
 *  '''Tpch Model:'''
 *  {{{
 *    FactTable = LineItem
 *    StarRelations: [
 *      LineItem - n:1 - Order => [[li_orderkey],[o_orderkey]]
 *      LineItem - n:1 - PartSupp => [[li_partkey, li_suppkey],[ps_partkey, ps_suppkey]]
 *      Order - n: 1 - Customer => [[o_custkey], [c_custkey]]
 *      PartSupp - n:1 - Part => [[ps_partkey], [p_partkey]]
 *      PartSupp - n:1 - Supplier => [[ps_suppkey], [s_suppkey]]
 *      Customer - n:1 - CustNation => [[c_nationkey], [cn_nationkey]]
 *      CustNation - n:1 - CustRegion => [[cn_regionkey], [cr_regionkey]]
 *      Supplier - n:1 - SupptNation => [[s_nationkey], [sn_nationkey]]
 *      SuppNation - n:1 - SuppRegion => [[sn_regionkey], [sr_regionkey]]
 *    ]
 *  }}}
 *
 *  Because of our restrictions we have had to model the ''Nation'' table as separate
 *  ''CustNation'' and ''SuppNation'' tables. Similar separation has to be done for ''CustRegion''
 *  and ''SuppRegion''. Having to setup separate entities for Supplier and Customer Nation is
 *  not atypical when directly writing SQLs; these would be views on the same Nation Dimension
 *  table. Currently we are being more restrictive than this, we require the 2 views to be
 *  tables in the Metastore(this is because during Plan rewrite we loose the Table association
 *  in [[AttributeReference Attributereferences]]. But note, this doesn't require the data to be
 *  copied, both tables can point to the same underlying data in the storage layer.
 *
 *  We have to rename the column names in the 2 Nation(and region) tables. This is so that we
 *  can infer the Attribute to Tables(in the Star Schema) associations in a Query Plan.
 *
 *
 * @param info the [[StarSchemaInfo]] used to build this StarSchema Graph.
 * @param factTable the node that represents the '''Fact Table'''
 * @param tableMap maps a tableName to the [[StarTable]] node in the StarSchema Graph.
 * @param attrMap provides a mapping from a columnName to its table.
 */
case class StarSchema(val info : StarSchemaInfo,
                 val factTable : StarTable,
                 val tableMap : Map[String, StarTable],
                 val attrMap : Map[String, StarTable]) {
  import StarSchema._

  /**
   * The seq of expressions representing one side of a join must all be AttributeReferences
   * and must be from the same table. If this condition is met, the table's name is returned.
   *
   * @param joinKeys
   * @return
   */
  def getUniqueTable(joinKeys : Seq[Expression]) : Option[String] = {

    val tables: Set[String] = {
      for (e <- joinKeys;
           a <- e.references
      ) yield attrMap.get(a.name).map(_.name).getOrElse(UNKNOWN_TABLE_NAME)
    }.toSet

    if (tables.size == 1 && tables.head != UNKNOWN_TABLE_NAME) tables.headOption else None
  }

  def isAttributeReference(e : Expression) = e.isInstanceOf[AttributeReference]

  /**
   * Does the join predicate represented by the left and right join keys match a join in the
   * StarSchema. So a join like {{{lineitem li join part p on li.l_partkey = p.p_partkey}}} is
   * represented as {{{Seq(AttributeReference("l_partkey")), Seq(AttributeReference("p_partkey"))}}}
   *
   * The following constraints must be met for the joining condition to be a join from this
   * StarSchema:
   *  - Every joining expressions can only be an AttributeReference
   *  - each set of joining conditions(leftJoinKeys, rightJoinKeys) must be on 1 table.
   *  - the 2 tables must be related in the StarSchema.
   *  - the matching Attributes in the input(leftJoinKeys, rightJoinKeys) must exactly
   *  nmatch the joining key defined in the StarSchema for the 2 tables involved.
   *
   * @param leftJoinKeys
   * @param rightJoinKeys
   * @return
   */
  def isStarJoin(leftJoinKeys : Seq[Expression], rightJoinKeys : Seq[Expression]) :
  Option[(String, String)] = {

    /*
     * joining expressions must be AttributeReferences
     */
    if ( !leftJoinKeys.forall(isAttributeReference) ||
      !rightJoinKeys.forall(isAttributeReference) ) {
      return None
    }

    /*
     * each set of joining conditions must be on 1 table.
     */
    val (leftTableName, rightTableName) =
      (getUniqueTable(leftJoinKeys), getUniqueTable(rightJoinKeys)) match {
        case (None, _) => return None
        case (_, None) => return None
        case (l,r) => (l.get,r.get)
      }

    var flip = false

    /*
     * the 2 tables must be related in the StarSchema.
     */
    val joinCondition = (tableMap(leftTableName), tableMap(rightTableName)) match {
      case(lT, rT) if (lT.parent.isDefined && lT.parent.get.tableName == rT.name) =>
        lT.parent.get.joiningKeys
      case(lT, rT) if (rT.parent.isDefined && rT.parent.get.tableName == lT.name) =>
        flip = true
        rT.parent.get.joiningKeys
      case _ => null
    }

    if ( joinCondition == null ) {
      return None
    }

    val lKeys = if (!flip) leftJoinKeys else rightJoinKeys
    val rKeys = if (!flip) rightJoinKeys else leftJoinKeys

    /*
     * form a list of tuples representing the joined columns from the 2 tables.
     */
    val joiningKeys = (lKeys.map(_.asInstanceOf[AttributeReference].name)).zip(
      rKeys.map(_.asInstanceOf[AttributeReference].name)
    ).toList

    /*
     * this list must match the joiningKeys set from the StarSchema.
     */

    if (joiningKeys.forall(joinCondition.contains(_)) &&
      joinCondition.forall(joiningKeys.contains(_)) ) {
      Some((leftTableName, rightTableName))
    } else {
      None
    }

  }

  def prettyString : String = {
    s"""
       |FactTable=${factTable.name}
       |${tableMap.mkString("\n")}
     """.stripMargin
  }

}

object StarSchema {

  val UNKNOWN_TABLE_NAME = "<unknownTable>"

  type JoinRelationInfo = (FunctionalDependencyType.Value, Set[(String, String)])

  type StarJoinGraph = collection.mutable.Map[String,
    collection.mutable.Map[String, JoinRelationInfo]]

  type ErrorInfo = String

  def flipJoinCondition(jCond : Set[(String, String)]) : Set[(String, String)] =
  jCond.map(t => (t._2, t._1))

  private def joinConditionToRelationInfo(jc : StarRelationInfo, reverse : Boolean = false) :
  JoinRelationInfo = {
    (jc.relationType,
      jc.joinCondition.map(c =>
        if (!reverse) {
          (c.leftAttribute, c.rightAttribute)
        } else {
          (c.rightAttribute, c.leftAttribute)
        }).toSet)
  }

  private def relations(joinGraph : StarJoinGraph, tableName : String) :
  collection.mutable.Map[String, JoinRelationInfo] = {
    if ( !joinGraph.contains(tableName)) {
      joinGraph(tableName) = collection.mutable.Map()
    }
    joinGraph(tableName)
  }

  private def addToJoinGraph(joinGraph : StarJoinGraph, jC : StarRelationInfo) :
  Either[ErrorInfo, Unit] = {
    val lrs = relations(joinGraph, jC.leftTable)

    jC.relationType match {
      case FunctionalDependencyType.ManyToOne =>  {
        if (lrs.contains(jC.rightTable)) {
          return Left(s"multiple join conditions for '${jC.leftTable}' and '${jC.rightTable}'")
        }
        lrs(jC.rightTable) = joinConditionToRelationInfo(jC, false)
      }
      case FunctionalDependencyType.OneToOne =>  {
        if (lrs.contains(jC.rightTable)) {
          return Left(s"multiple join conditions for '${jC.leftTable}' and '${jC.rightTable}'")
        }
        lrs(jC.rightTable) = joinConditionToRelationInfo(jC, false)
        relations(joinGraph, jC.rightTable)(jC.leftTable) = joinConditionToRelationInfo(jC, true)
      }
    }

    Right(())
  }

  def apply(sourceDFName : String, info : StarSchemaInfo)(implicit sqlContext : SQLContext) :
  Either[ErrorInfo, StarSchema] = {

    val joinGraph : StarJoinGraph = collection.mutable.Map()

    /**
     * Go over the [[StarRelationInfo]] from the info and form a mapping:
     * TableName -> RelatedTable -> [[JoinRelationInfo]]
     */
    val joinGraphAdditions = info.relations.map(addToJoinGraph(joinGraph, _))

    val errors = (ArrayBuffer[String]() /: joinGraphAdditions) {
    case (a, Left(err)) => a += err
    case (a, Right(_)) => a
    }

    if ( errors.size > 0) {
      return Left(errors.mkString("\n"))
    }

    val traversedEdges = collection.mutable.Set[(String, String)]()
    val tableMap = collection.mutable.Map[String, StarTable]()
    val attrMap = collection.mutable.Map[String, StarTable]()

    def addColumns(tabNm : String, tbl : StarTable) : Either[ErrorInfo, Unit] = {

      sqlContext.table(tabNm).schema.fieldNames.foreach { aName =>
        if ( attrMap.contains(aName) ) {
          return Left(s"Column $aName is not unique across Star Schema; " +
            s"in tables ${attrMap(aName).name}, $tabNm")
        }
        attrMap(aName) = tbl
      }
      return Right(())
    }

    def addTables(tables : Seq[String]) : Either[ErrorInfo, Unit] = {

      if (tables.isEmpty) return Right(())

      val descendantTables = ArrayBuffer[String]()

      tables.foreach { tName =>
        val childMap = joinGraph.getOrElse(tName, Map())
        childMap.foreach {
          case(childTable, jRInfo) => {
            if ( tableMap.contains(childTable)) {
              val edge = (childTable, tName)
              if ( !traversedEdges.contains(edge) ) {
                return Left(s"multiple join paths to table '$childTable'")
              }
            }
            val childStarTable = StarTable(childTable,
            Some(StarRelation(tName, jRInfo._1, flipJoinCondition(jRInfo._2))))
            tableMap(childTable) = childStarTable
            val r = addColumns(childTable, childStarTable)
            if (r.isLeft) return r
            descendantTables += childTable
          }
        }
      }

      addTables(descendantTables.toSeq)

    }

    tableMap(info.factTable) = StarTable(info.factTable, None)
    val ac = addColumns(sourceDFName, tableMap(info.factTable))
    if (ac.isLeft) {
      return Left(ac.left.get)
    }

    /**
     * Starting from the '''Fact Table''' recursively walk the Related tables, building out the
     * StarSchema with a [[StarTable]] node for each table. The following constraints are
     * enforced:
     *  - there is a unique Path to any Table.
     *  - the columnNames across the StarSchema are unique.
     */
    val r = addTables(Seq(info.factTable)).right.flatMap { x =>

      val tableSet = scala.collection.mutable.Set[String]()
      info.relations.foreach { r =>
        tableSet += r.leftTable
        tableSet += r.rightTable
      }

      val errors = (ArrayBuffer[String]() /: tableSet) {
        case (a, t) if (!tableMap.contains(t)) =>
          a += "Table '${t}' is not part of the join Graph"
        case (a, t) => a
      }

      if ( errors.size > 0) {
        return Left(errors.mkString("\n"))
      } else {
        Right(())
      }
    }

    r match {
      case Left(err) => Left(err)
      case _ => Right(new StarSchema(info,
        tableMap(info.factTable),
        tableMap.toMap,
        attrMap.toMap)
      )
    }

  }

}
