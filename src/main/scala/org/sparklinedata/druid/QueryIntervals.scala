package org.sparklinedata.druid

import com.github.nscala_time.time.Imports._
import org.sparklinedata.druid.metadata.DruidRelationInfo



case class QueryIntervals(drInfo : DruidRelationInfo,
                      intervals : List[Interval] = List()) {

  val indexIntervals = drInfo.druidDS.intervals

  def get : List[Interval] = if (intervals.isEmpty) indexIntervals else intervals

  private def indexInterval(dT : DateTime) : Option[Interval] = {
    indexIntervals.find(i => i.contains(dT))
  }

  private final def maxDate(d1 : DateTime, d2 : DateTime) : DateTime = {
    if ( d1 >= d2) d1 else d2
  }

  private def combine(i1 : Interval, i2 : Interval) : Interval = (i1, i2) match {
    case (i1, i2) if i1.start <= i2.start => new Interval(i1.start, maxDate(i1.end,i2.end))
    case _ => new Interval(i2.start, maxDate(i1.end,i2.end))
  }

  private def add(i : Interval) : QueryIntervals = {
    var added : Boolean = false

    val qIs = intervals.map {qI =>
      if ( qI.overlaps(i)) {
        added = true
        combine(qI, i)
      } else {
        qI
      }
    }

    if (!added) {
      QueryIntervals(drInfo, qIs :+ i)
    } else {
      QueryIntervals(drInfo, qIs)
    }

  }

  def gtCond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withStart(dT + 1.millis)
      add(cI)
    }
  }

  def gtECond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withStart(dT)
      add(cI)
    }
  }

  def ltCond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withEnd(dT)
      add(cI)
    }
  }

  def ltECond(dT : DateTime) : Option[QueryIntervals] = {
    indexInterval(dT).map { i =>
      val cI =  i.withEnd(dT+ 1.millis)
      add(cI)
    }
  }

}
