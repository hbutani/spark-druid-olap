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
    l@LogicalRelation(d@DruidRelation(info, None)))) =>
      Some(new DruidQueryBuilder(info))
  }

  val aggregateTransform: DruidTransform = {
    case (dqb, Aggregate(gEs, aEs, child)) => {
      plan(dqb, child).flatMap { dqb =>

        val dqb1 = gEs.foldLeft(Some(dqb).asInstanceOf[Option[DruidQueryBuilder]]) { (dqb, e) =>
          dqb.flatMap(groupingExpression(_, e))
        }

        val dqb2 = aEs.filter(a => !gEs.find(_.semanticEquals(a)).isDefined).foldLeft(dqb1) {
          (dqb, ne) =>
            dqb.flatMap(aggregateExpression(_, ne))
        }

        dqb2
      }
    }
  }


  def aggregateExpression(dqb: DruidQueryBuilder, ne: NamedExpression):
  Option[DruidQueryBuilder] = ne match {
    case Alias(Count(Literal(1, IntegerType)), a) =>
      Some(dqb.aggregate(FunctionAggregationSpec("count", a, "count")))
    case e@Alias(c: PartialAggregate, a) => (dqb, c) match {
      case CountDistinctAggregate(dN) =>
        Some(dqb.aggregate(new CardinalityAggregationSpec(a, List(dN))))
      case SumMinMaxAvgAggregate(t) => t._1 match {
        case "avg" => {
          val dC : DruidColumn = t._2
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
              new FieldAccessPostAggregationSpec(countAlias)), None))
          )
        }
        case _ => Some(dqb.aggregate(FunctionAggregationSpec(t._1, a, t._2.name)))
      }
      case _ => Some(dqb)
    }
    case _ => Some(dqb)
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
                   dqb.drInfo.sourceToDruidMapping.get(dNm) if dD.isInstanceOf[DruidDimension]
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
                   dM <- dqb.drInfo.sourceToDruidMapping.get(mNm) if dM.isInstanceOf[DruidMetric];
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
    case _ => Some(dqb)
  }
}
