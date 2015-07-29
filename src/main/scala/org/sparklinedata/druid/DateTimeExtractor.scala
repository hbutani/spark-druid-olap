package org.sparklinedata.druid

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ScalaUdf, Expression, Literal}
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.types.StringType
import org.sparklinedata.druid.metadata.{DruidDataSource, DruidRelationInfo}
import org.sparklinedata.spark.dateTime.Functions._

object PeriodExtractor {
  def unapply(e : Expression) : Option[Period] = e match {
    case ScalaUdf(fn, _, Seq(Literal(value, StringType))) if fn == periodFn => {
      Some(periodFn(value.toString))
    }
    case _ => None
  }
}

object DateTimeExtractor {
  def unapply(e : Expression) : Option[DateTime] = e match {
    case ScalaUdf(fn, _, Seq(Literal(value, StringType)))
      if fn == dateTimeFn => Some(dateTimeFn(value.toString))
    case ScalaUdf(fn, _, Seq(Literal(value, StringType)))
      if fn == dateTimeWithTZFn => Some(dateTimeWithTZFn(value.toString))
    case ScalaUdf(fn, _, Seq(Literal(value, StringType), Literal(pattern, StringType)))
      if fn == dateTimeWithFormatFn =>
      Some(dateTimeWithFormatFn(value.toString, pattern.toString))
    case ScalaUdf(fn, _, Seq(Literal(value, StringType), Literal(pattern, StringType)))
      if fn == dateTimeWithFormatAndTZFn =>
      Some(dateTimeWithFormatFn(value.toString, pattern.toString))
    case ScalaUdf(fn, _, Seq(DateTimeExtractor(dT), PeriodExtractor(p)))
      if fn == datePlusFn => Some(datePlusFn(dT, p))
    case ScalaUdf(fn, _, Seq(DateTimeExtractor(dT), PeriodExtractor(p)))
      if fn == dateMinusFn => Some(dateMinusFn(dT, p))
      // Todo add with functions here.
    case _ => None
  }

}

object IntervalConditionType extends Enumeration {
  val GT = Value
  val GTE = Value
  val LT = Value
  val LTE = Value
}

case class IntervalCondition(typ : IntervalConditionType.Value, dt : DateTime)

class IndexTimeReferenceExtractor(val dqb : DruidQueryBuilder) {

  val drInfo = dqb.drInfo

  def unapply(e : Expression) : Option[String] = e match {
    case ScalaUdf(fn, _, Seq(AttributeReference(nm, _, _, _)))
      if fn == dateTimeFn  => {
      val dC = dqb.druidColumn(nm)
      if ( dC.isDefined && dC.get.name == DruidDataSource.TIME_COLUMN_NAME ) Some(nm) else None
    }
    case _ => None
  }
}

class IntervalConditionExtractor(val dqb : DruidQueryBuilder) {

  val timeRefExtractor = new IndexTimeReferenceExtractor(dqb)

  def unapply(e : Expression) : Option[IntervalCondition] = e match {
    case ScalaUdf(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)))
      if fn == dateIsBeforeFn => Some(IntervalCondition(IntervalConditionType.LT, dT))
    case ScalaUdf(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)))
      if fn == dateIsBeforeOrEqualFn => Some(IntervalCondition(IntervalConditionType.LTE, dT))
    case ScalaUdf(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)))
      if fn == dateIsAfterFn => Some(IntervalCondition(IntervalConditionType.GT, dT))
    case ScalaUdf(fn, _, Seq(timeRefExtractor(timDm), DateTimeExtractor(dT)))
      if fn == dateIsAfterOrEqualFn => Some(IntervalCondition(IntervalConditionType.GTE, dT))
    case _ => None
  }
}