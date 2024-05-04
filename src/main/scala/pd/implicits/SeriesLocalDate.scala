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

import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.annotation.targetName

/**
  * Series extension for type java.time.LocalDate.
  *
  * @since 0.1.0
  */
class SeriesLocalDate private[pd] (data: SeriesData[LocalDate], index: BaseIndex, name: String)
    extends Series[LocalDate](data, index, name):

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(series: SeriesLocalDate): Series[Boolean] = map(series, _.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(series: SeriesLocalDate): Series[Boolean] = map(series, !_.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(series: SeriesLocalDate): Series[Boolean] = map(series, _.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(series: SeriesLocalDate): Series[Boolean] = map(series, !_.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(t: LocalDate): Series[Boolean] = map(_.isBefore(t))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(t: LocalDate): Series[Boolean] = map(!_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(t: LocalDate): Series[Boolean] = map(_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(t: LocalDate): Series[Boolean] = map(!_.isBefore(t))

  // *** GETTERS AND SETTERS ***

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
  def withYear(year: Int): Series[LocalDate] = map(_.withYear(year))

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
  def withMonth(month: Int): Series[LocalDate] = map(_.withMonth(month))

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
  def withDay(day: Int): Series[LocalDate] = map(_.withDayOfMonth(day))

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
  def withDayOfYear(day: Int): Series[LocalDate] = map(_.withDayOfYear(day))

  // *** OTHER METHODS ***

  /**
    * Concatenates date and time.
    *
    * @param hour
    *   Hour.
    * @param minute
    *   Minute.
    * @param second
    *   Second.
    * @return
    *   Series of type LocalDateTime.
    * @since 0.1.0
    */
  def atTime(hour: Int, minute: Int, second: Int): Series[LocalDateTime] = map(_.atTime(hour, minute, second))

  /**
    * Concatenates date and time.
    *
    * @param series
    *   Series of type LocalTime.
    * @return
    *   Series of type LocalDateTime.
    * @since 0.1.0
    */
  def atTime(series: Series[LocalTime]): Series[LocalDateTime] = map(series, _.atTime(_))

  /**
    * Concatenates date and time.
    *
    * @param time
    *   LocalTime.
    * @return
    *   Series of type LocalDateTime.
    * @since 0.1.0
    */
  def atTime(time: LocalTime): Series[LocalDateTime] = map(_.atTime(time))

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
  def minus(series: Series[Int], unit: TemporalUnit): Series[LocalDate] =
    map(series, _.minus(_, unit))

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
  def minus(series: Series[Long], unit: TemporalUnit): Series[LocalDate] =
    map(series, _.minus(_, unit))

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
  def minus(value: Long, unit: TemporalUnit = ChronoUnit.DAYS): Series[LocalDate] = map(_.minus(value, unit))

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
  def plus(series: Series[Int], unit: TemporalUnit): Series[LocalDate] = map(series, _.plus(_, unit))

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
  def plus(series: Series[Long], unit: TemporalUnit): Series[LocalDate] = map(series, _.plus(_, unit))

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
  def plus(value: Long, unit: TemporalUnit = ChronoUnit.DAYS): Series[LocalDate] = map(_.plus(value, unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param series
    *   Series with LocalDate.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(series: Series[LocalDate], unit: TemporalUnit): Series[Long] = map(series, _.until(_, unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param t
    *   LocalDate.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(t: LocalDate, unit: TemporalUnit): Series[Long] = map(_.until(t, unit))
