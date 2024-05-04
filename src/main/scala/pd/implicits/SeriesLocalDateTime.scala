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

import java.time.temporal.TemporalUnit
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.annotation.targetName

/**
  * Series extension for type java.time.LocalDateTime.
  *
  * @since 0.1.0
  */
class SeriesLocalDateTime private[pd] (data: SeriesData[LocalDateTime], index: BaseIndex, name: String)
    extends Series[LocalDateTime](data, index, name):

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(series: SeriesLocalDateTime): Series[Boolean] = map(series, _.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(series: SeriesLocalDateTime): Series[Boolean] = map(series, !_.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(series: SeriesLocalDateTime): Series[Boolean] = map(series, _.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(series: SeriesLocalDateTime): Series[Boolean] = map(series, !_.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(t: LocalDateTime): Series[Boolean] = map(_.isBefore(t))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(t: LocalDateTime): Series[Boolean] = map(!_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(t: LocalDateTime): Series[Boolean] = map(_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(t: LocalDateTime): Series[Boolean] = map(!_.isBefore(t))

  // *** GETTERS AND SETTERS ***

  /**
    * Gets the local date.
    *
    * @return
    *   The local date part.
    * @since 0.1.0
    */
  def toLocalDate: Series[LocalDate] = map(_.toLocalDate)

  /**
    * Gets the local time.
    *
    * @return
    *   The local time part.
    * @since 0.1.0
    */
  def toLocalTime: Series[LocalTime] = map(_.toLocalTime)

  /**
    * Gets the year.
    *
    * @return
    *   The year as integer value.
    * @since 0.1.0
    */
  def year: Series[Int] = map(_.getYear)

  /**
    * Sets the year.
    *
    * @param year
    *   The year as integer value.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withYear(year: Int): Series[LocalDateTime] = map(_.withYear(year))

  /**
    * Gets the month.
    *
    * @return
    *   The month as integer value ranging from 1 to 12.
    * @since 0.1.0
    */
  def month: Series[Int] = map(_.getMonthValue)

  /**
    * Sets the month.
    *
    * @param month
    *   The month as integer value ranging from 1 to 12.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withMonth(month: Int): Series[LocalDateTime] = map(_.withMonth(month))

  /**
    * Gets the day in the month.
    *
    * @return
    *   The day as integer value ranging from 1 to 31.
    * @since 0.1.0
    */
  def day: Series[Int] = map(_.getDayOfMonth)

  /**
    * Gets the day in the week as of the ISO-8601 standard, from 1 (Monday) to 7 (Sunday).
    *
    * @return
    *   The day as integer value ranging from 1 to 7.
    * @since 0.1.0
    */
  def dayOfWeek: Series[Int] = map(_.getDayOfWeek.getValue)

  /**
    * Gets the day in the year.
    *
    * @return
    *   The day as integer value ranging from 1 to 365 or 1 to 366 in a leap year.
    * @since 0.1.0
    */
  def dayOfYear: Series[Int] = map(_.getDayOfYear)

  /**
    * Sets the day of the month.
    *
    * @param day
    *   The day as integer value ranging from 1 to 31.
    * @return
    *   Series.
    * @throws DateTimeException
    *   if the day-of-month value is invalid.
    * @since 0.1.0
    */
  def withDay(day: Int): Series[LocalDateTime] = map(_.withDayOfMonth(day))

  /**
    * Sets the day of the year.
    *
    * @param day
    *   The day as integer value ranging from 1 to 365 or 1 to 366 in a leap year.
    * @return
    *   Series.
    * @throws DateTimeException
    *   if the day-of-month value is invalid.
    * @since 0.1.0
    */
  def withDayOfYear(day: Int): Series[LocalDateTime] = map(_.withDayOfYear(day))

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
  def withHour(hour: Int): Series[LocalDateTime] = map(_.withHour(hour))

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
  def withMinute(minute: Int): Series[LocalDateTime] = map(_.withMinute(minute))

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
  def withSecond(second: Int): Series[LocalDateTime] = map(_.withSecond(second))

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
  def withNano(nano: Int): Series[LocalDateTime] = map(_.withSecond(nano))

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
  def minus(series: Series[Int], unit: TemporalUnit): Series[LocalDateTime] = map(series, _.minus(_, unit))

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
  def minus(series: Series[Long], unit: TemporalUnit): Series[LocalDateTime] = map(series, _.minus(_, unit))

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
  def minus(value: Long, unit: TemporalUnit): Series[LocalDateTime] = map(_.minus(value, unit))

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
  def plus(series: Series[Int], unit: TemporalUnit): Series[LocalDateTime] = map(series, _.plus(_, unit))

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
  def plus(series: Series[Long], unit: TemporalUnit): Series[LocalDateTime] = map(series, _.plus(_, unit))

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
  def plus(value: Long, unit: TemporalUnit): Series[LocalDateTime] = map(_.plus(value, unit))

  /**
    * Truncates to the specified temporal unit.
    *
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def truncatesTo(unit: TemporalUnit): Series[LocalDateTime] = map(_.truncatedTo(unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param series
    *   Series with LocalDateTime.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(series: Series[LocalDateTime], unit: TemporalUnit): Series[Long] = map(series, _.until(_, unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param t
    *   LocalDateTime.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(t: LocalDateTime, unit: TemporalUnit): Series[Long] = map(_.until(t, unit))
