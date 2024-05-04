/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.io

import pd.Series

import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import scala.util.{Success, Try}

/**
  * Decoding of Series of strings to specific types.
  *
  * @since 0.1.0
  */
object StringDecoding:

  /** @since 0.1.0 */
  type StringDecoder = Series[String] => Try[Series[?]]

  /**
    * Decodes Series of strings automatically by attempting to process strings with multiple decoders. If decoding fails
    * for one row the next decoder in `decoders` is tried.
    *
    * @param series
    *   Series to decode.
    * @param decoders
    *   Array of decoders.
    * @return
    *   Series of decoded type.
    * @since 0.1.0
    */
  def autoDecode(series: Series[String], decoders: Array[StringDecoder] = defaultDecoders): Series[Any] =
    var result: Series[?] = series
    var converted = false
    val iterator = decoders.iterator
    while iterator.hasNext && !converted do
      val decoder = iterator.next()
      val attempt = decoder(series)
      if attempt.isSuccess then
        result = attempt.get
        converted = true
    result.toAny

  /**
    * Default decoders (order defines priority).
    *
    * @since 0.1.0
    */
  val defaultDecoders: Array[StringDecoder] = Array(
    decodeInt,
    decodeDouble,
    decodeLocalDate,
    decodeLocalTime,
    decodeLocalDateTime,
    decodeZonedDateTime,
  )

  /** @since 0.1.0 */
  def decodeDouble(series: Series[String]): Try[Series[Double]] = Try(series.toDouble)

  /** @since 0.1.0 */
  def decodeFloat(series: Series[String]): Try[Series[Float]] = Try(series.toFloat)

  /** @since 0.1.0 */
  def decodeInt(series: Series[String]): Try[Series[Int]] = Try(series.toInt)

  /** @since 0.1.0 */
  def decodeLong(series: Series[String]): Try[Series[Long]] = Try(series.toLong)

  /** @since 0.1.0 */
  def decodeLocalDate(series: Series[String]): Try[Series[LocalDate]] = Try(series.toLocalDate)

  /** @since 0.1.0 */
  def decodeLocalDateTime(series: Series[String]): Try[Series[LocalDateTime]] = Try(series.toLocalDateTime)

  /** @since 0.1.0 */
  def decodeLocalTime(series: Series[String]): Try[Series[LocalTime]] = Try(series.toLocalTime)

  /** @since 0.1.0 */
  def decodeString(series: Series[String]): Try[Series[String]] = Success(series)

  /** @since 0.1.0 */
  def decodeZonedDateTime(series: Series[String]): Try[Series[ZonedDateTime]] = Try(series.toZonedDateTime)
