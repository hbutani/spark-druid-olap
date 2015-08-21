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

import scala.collection.mutable.ArrayBuffer

object FunctionalDependencyType extends Enumeration {
  val OneToOne = Value("1-1")
  val ManyToOne = Value("n-1")
}

case class FunctionalDependency(col1 : String,
                                col2 : String,
                                `type` : FunctionalDependencyType.Value)

case class FunctionalDependencies(val dDS : DruidDataSource,
                              fds : List[FunctionalDependency],
                                  depGraph : DependencyGraph) {

  class Component(val base : Int) {
    var connectedNodes : List[Int] = List()

    def connected(nd : Int) : Boolean = {
      (base +: connectedNodes).foldLeft(false) { (b, c) =>
        b || depGraph.closure(c)(nd) != null
      }
    }
  }

  /**
   * Order the dimensions based on the number of relations they have. The ones with the
   * most relations are the base of Dimensional components. For e.g. in a Star Schema
   * with dimensions (Product, Customer) if p_name is the key for Product, it will be related
   * to all other product attributes. A query about p_name and other Product fields will be
   * bounded by the cardinality of p_name for the Product dimension. So for a query that has
   * a Group By on p_name, p_brand, p_mfr the cardinality estimate of the result set is
   * the cardinality of p_name.
   * <p>
   * Todo: for a query on p_brand, p_mfr bound the cardinality by the cardinality of p_name.
   * I thik this requires the user to specify the key column of a dimension.
   * @param dimNames
   * @return
   */
  def estimateCardinality(dimNames : List[String]) : Long = {

    val oDims : List[Int] = dimNames.map(dN => dDS.indexOfDimension(dN)).sortWith { (a,b) =>
      depGraph.closure(a).size > depGraph.closure(b).size
    }

    var components = List[Component]()

    oDims.foreach { d =>

      val c = components.find { c =>
        c.connected(d)
      }

      if ( c.isDefined) {
        c.get.connectedNodes = (c.get.connectedNodes :+ d)
      } else {
        components = (components :+ new Component(d))
      }

    }

    components.foldLeft(1L)((p, c) => p * dDS.dimensions(c.base).cardinality)

  }
}

case class DependencyGraph(val closure : Array[Array[FunctionalDependencyType.Value]]) {

  def debugString(dDS : DruidDataSource) : String = {
    val s = ArrayBuffer[String]()

    val n = dDS.dimensions.size
    (0 until n).foreach { i =>
      val d = dDS.dimensions(i)
      val descendants : Array[(String, FunctionalDependencyType.Value)] =
        closure(i).zipWithIndex.flatMap { t =>
          val d = t._1
          val j = t._2
        if (d != null) List((dDS.dimensions(j).name, d)) else Nil
      }
      val des = descendants.mkString(",")
      s += s"${d.name} -> ${des}"
    }

    s.mkString("\n")
  }
 }

object DependencyGraph {

  class Node(val d : DruidDimension, val idx : Int) {
    var incidentNodes : List[Relation] = List()

    def incident(nd : Node, fType : FunctionalDependencyType.Value): Unit = {
      incidentNodes = (incidentNodes :+ Relation(nd, fType))
    }

  }

  case class Relation(nd : Node, fType : FunctionalDependencyType.Value) {

    def &&(r : Relation) : Relation = {
      import FunctionalDependencyType._
      if ( r == null) return null
      else {
        (fType, r.fType) match {
          case (OneToOne, OneToOne) => Relation(r.nd, OneToOne)
          case (OneToOne, ManyToOne) => Relation(r.nd, ManyToOne)
          case (ManyToOne, OneToOne) => Relation(r.nd, ManyToOne)
          case (ManyToOne, ManyToOne) => Relation(r.nd, ManyToOne)
        }
      }
    }
  }

  /**
   * Capture the FunctionalDependency relations as a Graph.
   * Compute the closure of this relation.
   * @param dDS
   * @param fds
   */
  def apply(dDS : DruidDataSource,
            fds : List[FunctionalDependency]) : DependencyGraph = {

    var nodes: IndexedSeq[Node] = dDS.dimensions.zipWithIndex.map(t => new Node(t._1, t._2))

    fds.foreach { fd =>
      val nd1 = nodes(dDS.indexOfDimension(fd.col1))
      val nd2 = nodes(dDS.indexOfDimension(fd.col2))
      nd1.incident(nd2, fd.`type`)
    }

    val closure: Array[Array[FunctionalDependencyType.Value]] = {

      val n = dDS.dimensions.size

      val a = new Array[Array[Relation]](n)
      val b = new Array[Array[FunctionalDependencyType.Value]](n)

      (0 until n).foreach { i =>
        a(i) = new Array(n)
        b(i) = new Array(n)
      }

      (0 until n).foreach { i =>
        nodes(i).incidentNodes.foreach { r =>
          a(i)(r.nd.idx) = r
          b(i)(r.nd.idx) = r.fType
          if ( r.fType == FunctionalDependencyType.OneToOne ) {
            a(r.nd.idx)(i) = r
            b(r.nd.idx)(i) = r.fType
          }
        }

      }

      for (k <- 0 to (n - 1)) {
        for (i <- 0 to (n - 1)) {
          for (j <- 0 to (n - 1)) {
            a(i)(j) =
              if (a(i)(j) != null) a(i)(j) else if (a(i)(k) == null) null else a(i)(k) && a(k)(j)
            b(i)(j) = if ( a(i)(j) != null) a(i)(j).fType else null
          }
        }
      }

      b
    }

    DependencyGraph(closure)
  }

}
