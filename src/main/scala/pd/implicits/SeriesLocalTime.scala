/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.Series
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData

import java.time.LocalTime
import java.time.temporal.TemporalUnit
import scala.annotation.targetName

/**
  * Series extension for type java.time.LocalTime.
  *
  * @since 0.1.0
  */
class SeriesLocalTime private[pd] (data: SeriesData[LocalTime], index: BaseIndex, name: String)
    extends Series[LocalTime](data, index, name):

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(series: SeriesLocalTime): Series[Boolean] = map(series, _.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(series: SeriesLocalTime): Series[Boolean] = map(series, !_.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(series: SeriesLocalTime): Series[Boolean] = map(series, _.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(series: SeriesLocalTime): Series[Boolean] = map(series, !_.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(t: LocalTime): Series[Boolean] = map(_.isBefore(t))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(t: LocalTime): Series[Boolean] = map(!_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(t: LocalTime): Series[Boolean] = map(_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(t: LocalTime): Series[Boolean] = map(!_.isBefore(t))

  // *** GETTERS AND SETTERS ***

  /**
    * Gets the hour.
    *
    * @return
    *   The hour as integer value ranging from 0 to 23.
    * @since 0.1.0
    */
  def hour: Series[Int] = map(_.getHour)

  /**
    * Sets the hour.
    *
    * @param hour
    *   The hour as integer value ranging from 0 to 23.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withHour(hour: Int): Series[LocalTime] = map(_.withHour(hour))

  /**
    * Gets the minute.
    *
    * @return
    *   The minute as integer value ranging from 0 to 59.
    * @since 0.1.0
    */
  def minute: Series[Int] = map(_.getMinute)

  /**
    * Sets the minute.
    *
    * @param minute
    *   The minute as integer value ranging from 0 to 59.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withMinute(minute: Int): Series[LocalTime] = map(_.withMinute(minute))

  /**
    * Gets the second.
    *
    * @return
    *   The second as integer value ranging from 0 to 59.
    * @since 0.1.0
    */
  def second: Series[Int] = map(_.getSecond)

  /**
    * Sets the second.
    *
    * @param second
    *   The second as integer value ranging from 0 to 59.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withSecond(second: Int): Series[LocalTime] = map(_.withSecond(second))

  /**
    * Gets the nano seconds.
    *
    * @return
    *   The nano seconds as integer value ranging from 0 to 999,999,999.
    * @since 0.1.0
    */
  def nano: Series[Int] = map(_.getNano)

  /**
    * Sets the nano second.
    *
    * @param nano
    *   The nano second as integer value ranging from from 0 to 999,999,999.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withNano(nano: Int): Series[LocalTime] = map(_.withSecond(nano))

  // *** OTHER METHODS ***

  /**
    * Subtracts the value in the specified temporal unit.
    *
    * @param series
    *   Series with values to subtract.
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def minus(series: Series[Int], unit: TemporalUnit): Series[LocalTime] = map(series, _.minus(_, unit))

  /**
    * Subtracts the value in the specified temporal unit.
    *
    * @param series
    *   Series with values to subtract.
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  @targetName("minusLong")
  def minus(series: Series[Long], unit: TemporalUnit): Series[LocalTime] = map(series, _.minus(_, unit))

  /**
    * Subtracts the value in the specified temporal unit.
    *
    * @param value
    *   Value to subtract.
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def minus(value: Long, unit: TemporalUnit): Series[LocalTime] = map(_.minus(value, unit))

  /**
    * Adds the value in the specified temporal unit.
    *
    * @param series
    *   Series with values to add.
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def plus(series: Series[Int], unit: TemporalUnit): Series[LocalTime] = map(series, _.plus(_, unit))

  /**
    * Adds the value in the specified temporal unit.
    *
    * @param series
    *   Series with values to add.
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  @targetName("plusLong")
  def plus(series: Series[Long], unit: TemporalUnit): Series[LocalTime] = map(series, _.plus(_, unit))

  /**
    * Adds the value in the specified temporal unit.
    *
    * @param value
    *   Value to add.
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def plus(value: Long, unit: TemporalUnit): Series[LocalTime] = map(_.plus(value, unit))

  /**
    * Truncates to the specified temporal unit.
    *
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def truncatesTo(unit: TemporalUnit): Series[LocalTime] = map(_.truncatedTo(unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param series
    *   Series with LocalTime.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(series: Series[LocalTime], unit: TemporalUnit): Series[Long] = map(series, _.until(_, unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param t
    *   LocalTime.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(t: LocalTime, unit: TemporalUnit): Series[Long] = map(_.until(t, unit))
