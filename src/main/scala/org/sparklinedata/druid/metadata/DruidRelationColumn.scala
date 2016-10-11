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

import org.sparklinedata.druid.{DruidDataSourceException, Utils}

case class SpatialDruidDimensionInfo(
                                  druidColumn : String,
                                  spatialPosition : Int,
                                  minValue : Option[Double],
                                  maxValue : Option[Double]
                                )

case class SpatialDruidDimension(
                             druidColumn : DruidDimension,
                             spatialPosition : Int,
                             minValue : Option[Double],
                             maxValue : Option[Double]
                             )

case class DruidRelationColumnInfo(
                                  column : String,
                                  druidColumn : Option[String],
                                  spatialIndex : Option[SpatialDruidDimensionInfo] = None,
                                  hllMetric : Option[String] = None,
                                  sketchMetric : Option[String] = None,
                                  cardinalityEstimate : Option[Long] = None
                                  )

/**
  * Captures the link(s) of a source column to the Druid Index.
  *
  * A column ca have several kinds of links to the Druid Index:
  * - it can be a Druid Dimension, possibly the Time Dimension of the Druid Index.
  * - it can be a component(axis) of a Spatial Index in Druid
  * - its value can be stored as a HLL Aggregation in Druid.
  * - its value can be stored in a Sketch Aggregation in Druid.
  *
  * A column can have multiple links, for example:
  * - a latitude column can be both a Druid Dimension and be an axis in a Spatial Index.
  * - a column can be both a Dimension and have an HLL and/or Sketch.
  *
  * @param column the source column
  * @param druidColumn the direct link of this source coulmn to a Druid Dimension or Metric.
  * @param spatialIndex the spatial index for this column.
  * @param hllMetric the hll Metric for this column
  * @param sketchMetric the sketch for this column
  * @param cardinalityEstimate user provided cardinality estimate.
  */
case class DruidRelationColumn(
                              column : String,
                              druidColumn : Option[DruidColumn],
                              spatialIndex : Option[SpatialDruidDimension] = None,
                              hllMetric : Option[DruidMetric] = None,
                              sketchMetric : Option[DruidMetric] = None,
                              cardinalityEstimate : Option[Long] = None
                              ) {

  private lazy val druidColumnToUse : DruidColumn = {
    Utils.filterSomes(
      Seq(druidColumn, hllMetric, sketchMetric, spatialIndex.map(_.druidColumn)).toList
    ).head.get
  }

  def hasDirectDruidColumn = druidColumn.isDefined
  def hasSpatialIndex = spatialIndex.isDefined
  def hasHLLMetric = hllMetric.isDefined
  def hasSketchMetric = sketchMetric.isDefined

  def name = druidColumnToUse.name

  def dataType = druidColumnToUse.dataType

  def size = druidColumnToUse.size

  val cardinality : Long = cardinalityEstimate.getOrElse{
    if (cardinalityEstimate.isDefined) {
      cardinalityEstimate.get
    } else {
      druidColumnToUse.cardinality
    }
  }

  def isDimension(excludeTime : Boolean = false) : Boolean = {
    hasDirectDruidColumn && druidColumnToUse.isDimension(excludeTime)
  }

  def isTimeDimension : Boolean = {
    hasDirectDruidColumn &&  druidColumnToUse.isInstanceOf[DruidTimeDimension]
  }

  def isMetric : Boolean = hasDirectDruidColumn && !isDimension(false)

  def metric = druidColumnToUse.asInstanceOf[DruidMetric]
}

object DruidRelationColumn {

  def apply(druidDS: DruidDataSource,
            timeDimensionCol : String,
            colInfo: DruidRelationColumnInfo
           ) : Option[DruidRelationColumn] = {

    (colInfo.druidColumn, colInfo.spatialIndex, colInfo.hllMetric, colInfo.sketchMetric) match {
      case (Some(dC), None, None, None) if (dC == timeDimensionCol) => {
        val dColumn = druidDS.timeDimension.get
        Some(
          new DruidRelationColumn(colInfo.column,
            Some(dColumn),
            None, None, None,
            colInfo.cardinalityEstimate
          )
        )
      }
      case (Some(dC), None, None, None) if druidDS.columns.contains(dC) => {
        val dColumn = druidDS.columns(dC)
        Some(
          new DruidRelationColumn(colInfo.column,
          Some(dColumn),
          None, None, None,
          colInfo.cardinalityEstimate
        )
        )
      }
      case (None, Some(sI), None, None)
        if druidDS.columns.contains(sI.druidColumn) &&
          druidDS.columns(sI.druidColumn).isDimension() => {
        Some(
        new DruidRelationColumn(colInfo.column,
          None,
          Some(
            SpatialDruidDimension(druidDS.columns(sI.druidColumn).asInstanceOf[DruidDimension],
            sI.spatialPosition, sI.minValue, sI.maxValue)
          ),
          None, None,
          colInfo.cardinalityEstimate
        )
        )
      }
      case (None, None, hllMetric, sketchMetric)
        if hllMetric.isDefined || sketchMetric.isDefined => {

        var hllM : Option[DruidMetric] = None
        var sketchM : Option[DruidMetric] = None

        if ( hllMetric.isDefined) {
          if (!druidDS.columns.contains(hllMetric.get) ||
            druidDS.columns(hllMetric.get).isDimension() ) {
            return None
          }
          hllM = Some(druidDS.columns(hllMetric.get).asInstanceOf[DruidMetric])
        }

        if ( sketchM.isDefined) {
          if (!druidDS.columns.contains(sketchMetric.get) ||
            druidDS.columns(sketchMetric.get).isDimension() ) {
            return None
          }
          sketchM = Some(druidDS.columns(sketchMetric.get).asInstanceOf[DruidMetric])
        }

        if (hllM.isDefined || sketchM.isDefined) {
          Some(new DruidRelationColumn(colInfo.column,
            None,
            None,
            hllM, sketchM,
            colInfo.cardinalityEstimate
          ))
        } else {
          None
        }

      }
      case _ => None
    }

  }

  def apply(dC : DruidColumn) : DruidRelationColumn = {
    new DruidRelationColumn(dC.name,
      Some(dC),
      None,
      None, None,
      None
    )
  }
}
