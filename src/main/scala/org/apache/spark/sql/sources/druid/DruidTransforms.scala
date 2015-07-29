package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.sources.LogicalRelation
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.sparklinedata.druid.metadata.{DruidColumn, DruidDimension, DruidDataType, DruidMetric}
import org.sparklinedata.druid._


abstract class DruidTransforms {
  self: DruidPlanner =>

  type DruidTransform = PartialFunction[(DruidQueryBuilder, LogicalPlan), Option[DruidQueryBuilder]]

  val druidRelationTransform: DruidTransform = {
    case (_, PhysicalOperation(projectList, filters,
    l@LogicalRelation(d@DruidRelation(info, None)))) => {
      var dqb : Option[DruidQueryBuilder] = Some(DruidQueryBuilder(info))
      dqb = projectList.foldLeft(dqb) { (dqB, e) =>
        dqB.flatMap(projectExpression(_, e))
      }

      if ( dqb.isDefined) {
        val iCE: IntervalConditionExtractor = new IntervalConditionExtractor(dqb.get)
        filters.foldLeft(dqb) { (dqB, e) =>
          dqB.flatMap(filterExpression(_, iCE, e))
        }
      } else None
    }
  }

  val aggregateTransform: DruidTransform = {
    case (dqb, agg@Aggregate(gEs, aEs, child)) => {
      plan(dqb, child).flatMap { dqb =>

        val dqb1 = gEs.foldLeft(Some(dqb).asInstanceOf[Option[DruidQueryBuilder]]) { (dqb, e) =>
          dqb.flatMap(groupingExpression(_, e))
        }

        val allAggregates =
          aEs.flatMap(_ collect { case a: AggregateExpression => a})
        // Collect all aggregate expressions that can be computed partially.
        val partialAggregates =
          aEs.flatMap(_ collect { case p: PartialAggregate => p})

        // Only do partial aggregation if supported by all aggregate expressions.
        if (allAggregates.size == partialAggregates.size) {
          val dqb2 = partialAggregates.foldLeft(dqb1) {
            (dqb, ne) =>
              dqb.flatMap(aggregateExpression(_, ne))
          }

          dqb2.map(_.aggregateOp(agg))
        } else {
          None
        }

      }
    }
  }


  def aggregateExpression(dqb: DruidQueryBuilder, pa: PartialAggregate):
  Option[DruidQueryBuilder] = pa match {
    case Count(Literal(1, IntegerType)) => {
      val a = dqb.nextAlias
      Some(dqb.aggregate(FunctionAggregationSpec("count", a, "count")).
        outputAttribute(a, pa, pa.dataType, LongType))
    }
    case c => {
      val a = dqb.nextAlias
      (dqb, c) match {
        case CountDistinctAggregate(dN) =>
          Some(dqb.aggregate(new CardinalityAggregationSpec(a, List(dN))).
            outputAttribute(a, pa, pa.dataType, LongType))
        case SumMinMaxAvgAggregate(t) => t._1 match {
          case "avg" => {
            val dC: DruidColumn = t._2
            val aggFunc = dC.dataType match {
              case DruidDataType.Long => "longSum"
              case _ => "doubleSum"
            }
            val sumAlias = dqb.nextAlias
            val countAlias = dqb.nextAlias

            Some(
              dqb.aggregate(FunctionAggregationSpec(aggFunc, sumAlias, dC.name)).
                aggregate(FunctionAggregationSpec("count", countAlias, "count")).
                postAggregate(new ArithmeticPostAggregationSpec(a, "/",
                List(new FieldAccessPostAggregationSpec(sumAlias),
                  new FieldAccessPostAggregationSpec(countAlias)), None)).
                outputAttribute(a, pa, pa.dataType, DoubleType)
            )
          }
          case _ => {
            val dC: DruidColumn = t._2
            Some(dqb.aggregate(FunctionAggregationSpec(t._1, a, dC.name)).
              outputAttribute(a, pa, pa.dataType, DruidDataType.sparkDataType(dC.dataType))
            )
          }
        }
        case _ => None
      }
    }
  }

  private def attributeRef(arg: Expression): Option[String] = arg match {
    case AttributeReference(name, _, _, _) => Some(name)
    case Cast(AttributeReference(name, _, _, _), _) => Some(name)
    case _ => None
  }

  private object CountDistinctAggregate {
    def unapply(t: (DruidQueryBuilder, PartialAggregate)): Option[(String)]
    = {
      val dqb = t._1
      val pa = t._2

      val r = for (c <- pa.children.headOption if (pa.children.size == 1);
                   dNm <- attributeRef(c);
                   dD <-
                   dqb.druidColumn(dNm) if dD.isInstanceOf[DruidDimension]
      ) yield (pa, dD)

      r flatMap {
        case (p: CountDistinct, dD) if dqb.drInfo.allowCountDistinct => Some(dD.name)
        case (p: ApproxCountDistinct, dD) if dqb.drInfo.allowCountDistinct => Some(dD.name)
        case _ => None
      }
    }
  }

  /**
   * 1. PartialAgg must have only 1 arg.
   * 2. The arg must be an AttributeRef or a Cast of an AttributeRef
   * 3. Attribute must be a DruidMetric.
   * 4. The dataType of the PartialAgg must match the DruidMetric DataType, except for Avg.
   */
  private object SumMinMaxAvgAggregate {

    def unapply(t: (DruidQueryBuilder, PartialAggregate)): Option[(String, DruidColumn)]
    = {
      val dqb = t._1
      val pa = t._2

      val r = for (c <- pa.children.headOption if (pa.children.size == 1);
                   mNm <- attributeRef(c);
                   dM <- dqb.druidColumn(mNm) if dM.isInstanceOf[DruidMetric];
                   mDT <- Some(DruidDataType.sparkDataType(dM.dataType));
                   commonType <- HiveTypeCoercion.findTightestCommonType(pa.dataType, mDT)
                   if (commonType == mDT || pa.isInstanceOf[Average])
      ) yield (pa, commonType, dM)

      r flatMap {
        case (p: Sum, LongType, dM) => Some(("longSum", dM))
        case (p: Sum, DoubleType, dM) => Some(("doubleSum", dM))
        case (p: Min, LongType, dM) => Some(("longMin", dM))
        case (p: Min, DoubleType, dM) => Some(("doubleMin", dM))
        case (p: Max, LongType, dM) => Some(("longMax", dM))
        case (p: Max, DoubleType, dM) => Some(("doubleMax", dM))
        case (p: Average, LongType, dM) => Some(("avg", dM))
        case (p: Average, DoubleType, dM) => Some(("avg", dM))
        case _ => None
      }
    }


  }

  def groupingExpression(dqb: DruidQueryBuilder, ge: Expression):
  Option[DruidQueryBuilder] = ge match {
    case AttributeReference(nm, dT, _, _) => {
      for(dD <- dqb.druidColumn(nm) if dD.isInstanceOf[DruidDimension] )
        yield dqb.dimension(new DefaultDimensionSpec(dD.name, nm)).
          outputAttribute(nm, ge, ge.dataType, DruidDataType.sparkDataType(dD.dataType))
    }
    case _ => None
  }

  def projectExpression(dqb: DruidQueryBuilder, pe: Expression):
  Option[DruidQueryBuilder] = pe match {
    case AttributeReference(nm, dT, _, _) if dqb.druidColumn(nm).isDefined
    => Some(dqb)
    case Alias(ar@AttributeReference(nm1, dT, _, _), nm) => {
      for(dqbc <- projectExpression(dqb, ar))
        yield dqbc.addAlias(nm, nm1)
    }
    case _ => None
  }

  def filterExpression(dqb: DruidQueryBuilder, iCE : IntervalConditionExtractor, fe: Expression):
  Option[DruidQueryBuilder] = fe match {
    case iCE(iC) => dqb.interval(iC)
    case _ => None
  }
}
