/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.implicits

import pd.Series

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Implicit conversion of Collection types to Series.
  *
  * @since 0.1.0
  */
object Collections:

  /**
    * Implicit conversion of type Array.
    *
    * @param collection
    *   Array.
    * @return
    *   Series.
    * @since 0.1.0
    */
  implicit def array[T: ClassTag](collection: Array[T]): Series[T] = Series(collection*)
  implicit def array(collection: Array[Boolean]): SeriesBoolean = Series(collection*)
  implicit def array(collection: Array[Int]): SeriesInt = Series(collection*)
  implicit def array(collection: Array[Double]): SeriesDouble = Series(collection*)
  implicit def array(collection: Array[String]): SeriesString = Series(collection*)

  /**
    * Implicit conversion of type Seq.
    *
    * @param collection
    *   Sequence.
    * @return
    *   Series.
    * @since 0.1.0
    */
  implicit def seq[T: ClassTag](collection: Seq[T]): Series[T] = Series(collection*)
  implicit def seq(collection: Seq[Boolean]): SeriesBoolean = Series(collection*)
  implicit def seq(collection: Seq[Int]): SeriesInt = Series(collection*)
  implicit def seq(collection: Seq[Double]): SeriesDouble = Series(collection*)
  implicit def seq(collection: Seq[String]): SeriesString = Series(collection*)
