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

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.temporal.TemporalUnit
import java.util.Objects
import scala.annotation.targetName

/**
  * Series extension for type java.time.ZonedDateTime.
  *
  * @since 0.1.0
  */
class SeriesZonedDateTime private[pd] (data: SeriesData[ZonedDateTime], index: BaseIndex, name: String)
    extends Series[ZonedDateTime](data, index, name):

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(series: SeriesZonedDateTime): Series[Boolean] = map(series, _.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(series: SeriesZonedDateTime): Series[Boolean] = map(series, !_.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(series: SeriesZonedDateTime): Series[Boolean] = map(series, _.isAfter(_))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(series: SeriesZonedDateTime): Series[Boolean] = map(series, !_.isBefore(_))

  /** @since 0.1.0 */
  @targetName("isBefore")
  def <(t: ZonedDateTime): Series[Boolean] = map(_.isBefore(t))

  /** @since 0.1.0 */
  @targetName("isNotAfter")
  def <=(t: ZonedDateTime): Series[Boolean] = map(!_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isAfter")
  def >(t: ZonedDateTime): Series[Boolean] = map(_.isAfter(t))

  /** @since 0.1.0 */
  @targetName("isNotBefore")
  def >=(t: ZonedDateTime): Series[Boolean] = map(!_.isBefore(t))

  // *** GETTERS AND SETTERS ***

  /**
    * Gets the time zone offset.
    *
    * @return
    *   The ZoneOffset.
    * @since 0.1.0
    */
  def offset: Series[ZoneOffset] = map(_.getOffset)

  /**
    * Gets the time zone.
    *
    * @return
    *   The ZoneId.
    * @since 0.1.0
    */
  def zone: Series[ZoneId] = map(_.getZone)

  /**
    * Sets a different time-zone retaining the local date-time if possible.
    *
    * @param zone
    *   The time zone to change to.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withZoneSameLocal(zone: ZoneId): Series[ZonedDateTime] = map(_.withZoneSameLocal(zone))

  /**
    * Sets a different time-zone keeping the same instant in time.
    *
    * @param zone
    *   The time zone to change to.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withZoneSameInstant(zone: ZoneId): Series[ZonedDateTime] = map(_.withZoneSameInstant(zone))

  /**
    * Sets the time zone to the offset.
    *
    * @return
    *   Series.
    * @since 0.1.0
    */
  def withFixedOffsetZone(zone: ZoneId): Series[ZonedDateTime] = map(_.withFixedOffsetZone)

  /**
    * Gets the local date.
    *
    * @return
    *   The local date part.
    * @since 0.1.0
    */
  def toLocalDate: Series[LocalDate] = map(_.toLocalDate)

  /**
    * Gets the local date time.
    *
    * @return
    *   The local date time part.
    * @since 0.1.0
    */
  def toLocalDateTime: Series[LocalDateTime] = map(_.toLocalDateTime)

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
  def withYear(year: Int): Series[ZonedDateTime] = map(_.withYear(year))

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
  def withMonth(month: Int): Series[ZonedDateTime] = map(_.withMonth(month))

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
  def withDay(day: Int): Series[ZonedDateTime] = map(_.withDayOfMonth(day))

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
  def withDayOfYear(day: Int): Series[ZonedDateTime] = map(_.withDayOfYear(day))

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
  def withHour(hour: Int): Series[ZonedDateTime] = map(_.withHour(hour))

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
  def withMinute(minute: Int): Series[ZonedDateTime] = map(_.withMinute(minute))

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
  def withSecond(second: Int): Series[ZonedDateTime] = map(_.withSecond(second))

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
  def withNano(nano: Int): Series[ZonedDateTime] = map(_.withSecond(nano))

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
  def minus(series: Series[Int], unit: TemporalUnit): Series[ZonedDateTime] = map(series, _.minus(_, unit))

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
  def minus(series: Series[Long], unit: TemporalUnit): Series[ZonedDateTime] = map(series, _.minus(_, unit))

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
  def minus(value: Long, unit: TemporalUnit): Series[ZonedDateTime] = map(_.minus(value, unit))

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
  def plus(series: Series[Int], unit: TemporalUnit): Series[ZonedDateTime] = map(series, _.plus(_, unit))

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
  def plus(series: Series[Long], unit: TemporalUnit): Series[ZonedDateTime] = map(series, _.plus(_, unit))

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
  def plus(value: Long, unit: TemporalUnit): Series[ZonedDateTime] = map(_.plus(value, unit))

  /**
    * Truncates to the specified temporal unit.
    *
    * @param unit
    *   Temporal unit.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def truncatesTo(unit: TemporalUnit): Series[ZonedDateTime] = map(_.truncatedTo(unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param series
    *   Series with ZonedDateTime.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(series: Series[ZonedDateTime], unit: TemporalUnit): Series[Long] = map(series, _.until(_, unit))

  /**
    * Temporal difference in specified temporal unit.
    *
    * @param t
    *   ZonedDateTime.
    * @param unit
    *   Temporal unit measuring the time span.
    * @return
    *   Series.
    * @since 0.1.0
    */
  def until(t: ZonedDateTime, unit: TemporalUnit): Series[Long] = map(_.until(t, unit))
