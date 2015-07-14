package org.sparklinedata.druid.metadata

object FunctionalDependencyType extends Enumeration {
  val OneToOne = Value("1-1")
  val ManyToOne = Value("n-1")
}

case class FunctionalDependency(col1 : String,
                                col2 : String,
                                `type` : FunctionalDependencyType.Value)

class FunctionalDependencies(val dDS : DruidDataSource,
                              fds : List[FunctionalDependency]) {

  val depGraph : DependencyGraph = DependencyGraph(dDS, fds)

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

case class DependencyGraph(val closure : Array[Array[FunctionalDependencyType.Value]])

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

      (0 to n).foreach { i =>
        a(i) = new Array(n)
        b(i) = new Array(n)

        nodes(i).incidentNodes.foreach { r =>
          a(i)(r.nd.idx) = r
          b(i)(r.nd.idx) = r.fType
        }
      }

      for (k <- 0 to (n - 1)) {
        for (i <- 0 to (n - 1)) {
          for (j <- 0 to (n - 1)) {
            a(i)(j) = if (a(i)(j) != null) a(i)(j) else (a(i)(k) && a(k)(j))
            b(i)(j) = a(i)(j).fType
          }
        }
      }

      b
    }

    DependencyGraph(closure)
  }

}