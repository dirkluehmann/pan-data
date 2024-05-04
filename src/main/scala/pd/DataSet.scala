/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.IllegalOperation
import pd.internal.index.{BaseIndex, ColIndex, IndexOps}
import pd.internal.series.SeriesData

import scala.language.implicitConversions
import scala.reflect.classTag

/**
  * A DataSet similar to a DataFrame but with static column names and column types that can be checked at compile time.
  *
  * @tparam T
  *   Case class that extends DataSet.
  * @example
  * {{{
  * case class Members(
  *   firstName: Series[String],
  *   familyName: Series[String],
  *   fee: Series[Double],
  * ) extends DataSet[Members]
  * }}}
  * @see
  *   [[https://pan-data.org/scala/basics/dataset.html]]
  * @since 0.1.0
  */
abstract class DataSet[T <: Product with DataSet[T]] extends IndexOps[T]:
  private val instance: T =
    try this.asInstanceOf[T]
    catch case _: ClassCastException => throw new RuntimeException("The DataSet must extend case class T.")
  private[pd] val colIndex: ColIndex = DataSet.createColIndex(instance)
  private[pd] val index: BaseIndex = DataSet.createIndex(instance)

  /**
    * Converts the DataSet into a regular DataFrame object.
    *
    * @return
    *   DataFrame.
    * @since 0.1.0
    */
  inline def toDf: DataFrame = DataFrame.create(colIndex, index)

  // *** PRIVATE ***

  /**
    * Creates an instance by applying the index (implements IndexOps interface).
    *
    * @param index
    *   New index.
    * @return
    *   New DataSet instance of type specialized type T.
    * @since 0.1.0
    */
  private[pd] def withIndex(index: BaseIndex): T = convert(DataFrame.create(colIndex, index))

  /**
    * Converts a derived DataFrame back into a DataSet by applying Java runtime reflection.
    *
    * @param df
    *   DataFrame.
    * @return
    *   DataSet of specialized type T.
    * @since 0.1.0
    */
  private[pd] def convert(df: DataFrame): T =
    val seriesClass = classTag[Series[?]].runtimeClass
    val clazz: Class[?] = getClass
    val methods =
      clazz.getDeclaredMethods.filter(m => m.getName == "copy" && m.getParameterCount == instance.productArity)
    if methods.length == 1 then
      val copyMethod = methods.head
      copyMethod.getParameterTypes.foreach(c =>
        if (c != seriesClass) throw new RuntimeException("A DataSet may only contain Series.")
      )
      val seq = instance.productElementNames.map(df(_)).toSeq
      copyMethod.setAccessible(true)
      try copyMethod.invoke(this, seq*).asInstanceOf[T]
      catch
        case e: IllegalAccessException =>
          throw new RuntimeException(s"Cannot invoke copy method $copyMethod", e)
    else throw new RuntimeException("A DataSet must contain at least one column.")

/**
  * @see
  *   [[https://pan-data.org/scala/basics/dataset.html]]
  * @since 0.1.0
  */
object DataSet:
  /**
    * Converts a DataSet into a regular DataFrame object.
    *
    * @param ds:
    *   DataSet.
    * @return
    *   DataFrame.
    * @since 0.1.0
    */
  implicit def toDataFrame[T <: Product with DataSet[T]](ds: DataSet[T]): DataFrame =
    DataFrame.create(ds.colIndex, ds.index)

  // *** PRIVATE ***

  /**
    * Creates the column index.
    *
    * @param instance
    *   Instance of DataSet.
    * @return
    *   ColIndex.
    * @since 0.1.0
    */
  private def createColIndex(instance: Product): ColIndex =
    ColIndex.toMap(
      instance.productElementNames
        .zip(instance.productIterator)
        .toSeq
        .map(e => {
          if !e._2.isInstanceOf[Series[?]] then throw new RuntimeException("A DataSet may only contain Series.")
          (e._1, e._2.asInstanceOf[Series[?]].data)
        })
    )

  /**
    * Creates the row index.
    *
    * @param instance
    *   Instance of DataSet.
    * @return
    *   BaseIndex.
    * @since 0.1.0
    */
  private def createIndex(instance: Product): BaseIndex =
    if instance.productArity == 0 then throw new RuntimeException("A DataSet must contain at least one column.")
    val index = instance.productElement(0).asInstanceOf[Series[?]].index
    val haveSameIndex = instance.productIterator.forall(s => {
      val i = s.asInstanceOf[Series[?]].index
      index.eq(i) || (index.hasSameBase(i) && index.equals(i))
    })
    if haveSameIndex then index else throw new RuntimeException("All columns in a DataSet must have the same index.")
