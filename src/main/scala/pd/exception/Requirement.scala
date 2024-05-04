/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.exception

import pd.exception.{ColumnNotFoundException, RequirementException}
import pd.internal.utils.RequireType
import pd.{DataFrame, Series}

import scala.reflect.{ClassTag, Typeable}

class Requirement(df: DataFrame, _all: Boolean = false, _strict: Boolean = false):

  /**
    * If called, subsequent methods (by default) require that all values are defined.
    *
    * @return
    *   Requirement.
    * @since 0.1.0
    */
  def all: Requirement = Requirement(df, true, _strict)

  /**
    * Raises an exception if the column does not fulfill the check.
    *
    * @param col
    *   Column to be checked.
    * @param condition
    *   Predicate function to be checked.
    * @param all
    *   If true, all values of the column must be defined.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def check[T: ClassTag: Typeable: RequireType](
      col: String,
      condition: T => Boolean,
      all: Boolean = _all,
  ): Requirement =
    isType[T](col, all)
    raiseOnErrorTyped[T](col, _.forall(condition), "Condition failed.")

  /**
    * Raises an exception if the column does not fulfill the check or values are undefined.
    *
    * @param col
    *   Column name.
    * @param condition
    *   Predicate function to be checked.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def checkAll[T: ClassTag: Typeable: RequireType](col: String, condition: T => Boolean): Requirement =
    check[T](col, condition, true)

  /**
    * Raises an exception if one of the column names is missing.
    *
    * @param cols
    *   Columns names that are required.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def has(cols: String*): Requirement =
    cols.foreach(col => raiseOnError(col, _ => true))
    this

  /**
    * Raises an exception if column names are not matching the given list, i.e. missing or additional columns names with
    * arbitrary order.
    *
    * @param cols
    *   Columns names that are required.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def hasExactly(cols: String*): Requirement =
    if (cols.length != cols.distinct.length)
      throw RequirementException(s"list ${cols.mkString(", ")}", "Elements are not distinct.")
    cols.foreach(col => raiseOnError(col, _ => true))
    if df.numCols > cols.length then
      val extra = df.columns.filterNot(cols.contains).mkString(", ")
      throw RequirementException(s"list ${cols.mkString(", ")}", s"Found unexpected columns: $extra.")
    this

  /**
    * Raises an exception if the number of columns does not match.
    *
    * @param n
    *   Expected number of columns
    * @return
    *   Requirement.
    * @since 0.1.0
    */
  def hasNumCols(n: Int): Requirement =
    if (n != df.numCols)
      throw RequirementException(s"Expected $n columns but found ${df.numCols} columns.")
    else
      this

  /**
    * Raises an exception if the number of rows does not match.
    *
    * @param n
    *   Expected number of rows
    * @return
    *   Requirement.
    * @since 0.1.0
    */
  def hasNumRows(n: Int): Requirement =
    if (n != df.numRows)
      throw RequirementException(s"Expected $n rows but found ${df.numRows} rows.")
    else
      this

  /**
    * Raises an exception if column is missing or values do not match.
    *
    * @param col
    *   Column to be checked.
    * @param series
    *   Required values. If the Series has a name, it is ignored.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def equalsCol[T: ClassTag](col: String, series: Series[T]): Requirement =
    has(col)
    val s = df(col)
    val expected = series.as(col)
    if s.typeMatch[T](expected) then if s.equals(expected) then this else throw RequirementException(s, expected)
    else throw RequirementException(s, s"Expected type ${expected.typeString} but found type ${s.typeString}.")

  /**
    * Raises an exception if column is missing or values do not match.
    *
    * @param series
    *   Expected column where the column name is the name of the Series .
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def equalsCol[T: ClassTag](series: Series[T]): Requirement = equalsCol(series.name, series)

  /**
    * Raises an exception if column is missing or values do not match.
    *
    * @param col
    *   Column to be checked.
    * @param values
    *   Required values.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def equalsCol[T: ClassTag](col: String)(values: (T | Null)*): Requirement =
    equalsCol(col, Series(col)(values*))

  /**
    * Raises an exception if the DataFrame does not match the expected DataFrame.
    *
    * @param df
    *   Expected DataFrame.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def equals(df: DataFrame): Requirement =
    if this.df.equals(df) then this else throw RequirementException(this.df, df)

  /**
    * Raises an exception if the column is not of type `T`.
    *
    * @param col
    *   Column to be checked.
    * @param all
    *   If true, raises also an exception if values are undefined.
    * @tparam T
    *   Required type.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def isType[T: Typeable: RequireType](col: String, all: Boolean = _all): Requirement =
    if _strict then if all then raiseOnError(col, _.isTypeAllStrictly[T]) else raiseOnError(col, _.isTypeStrictly[T])
    else if all then raiseOnError(col, _.isTypeAll[T], "Incorrect type or contains undefined values.")
    else raiseOnError(col, _.isType[T], "Incorrect type.")

  /**
    * Raises an exception if the columns are not of type `T`.
    *
    * @param cols
    *   Columns to be checked.
    * @tparam T
    *   Required type.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def isType[T: Typeable: RequireType](cols: String*): Requirement =
    cols.foreach(isType(_, _all))
    this

  /**
    * Raises an exception if the columns are not of type `T` or have undefined values.
    *
    * @param cols
    *   Columns to be checked.
    * @tparam T
    *   Required type.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def isTypeAll[T: Typeable: RequireType](cols: String*): Requirement =
    cols.foreach(isType[T](_, true))
    this

  /**
    * Raises an exception if the columns are not of type `T` or have undefined values. The type check is performed on
    * all values which may cause a slower performance.
    *
    * @param cols
    *   Columns to be checked.
    * @tparam T
    *   Required type.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def isTypeAllStrictly[T: Typeable: RequireType](cols: String*): Requirement =
    cols.foreach(isTypeStrictly[T](_, true))
    this

  /**
    * Raises an exception if the columns are not of type `T`. The type check is performed on all values which may cause
    * a slower performance.
    *
    * @param col
    *   Column to be checked.
    * @param all
    *   If true, raises also an exception if values are undefined.
    * @tparam T
    *   Required type.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def isTypeStrictly[T: Typeable: RequireType](col: String, all: Boolean = _all): Requirement =
    if all then raiseOnError(col, _.isTypeAllStrictly[T], "Incorrect type.")
    else raiseOnError(col, _.isTypeStrictly[T], "Incorrect type or contains undefined values.")

  /**
    * Raises an exception if the columns are not of type `T`. The type check is performed on all values which may cause
    * a slower performance.
    *
    * @param cols
    *   Columns to be checked.
    * @tparam T
    *   Required type.
    * @return
    *   Requirement.
    * @throws RequirementException
    *   If the requirement fails.
    * @since 0.1.0
    */
  def isTypeStrictly[T: Typeable: RequireType](cols: String*): Requirement =
    cols.foreach(isTypeStrictly[T](_, _all))
    this

  /**
    * If called, subsequent methods (by default) perform type checks on all values which may cause a slower performance.
    *
    * @return
    *   Requirement.
    * @since 0.1.0
    */
  def strictly: Requirement = Requirement(df, _all, true)

  /**
    * Raises an exception if predicate function evaluates to false.
    *
    * @param col
    *   Column to be checked.
    * @param cond
    *   Predicate function.
    * @param msg
    *   Error message.
    * @return
    *   Requirement.
    * @since 0.1.0
    */
  private def raiseOnError(col: String, cond: Series[Any] => Boolean, msg: String = ""): Requirement =
    try if cond(df(col)) then this else throw RequirementException(df(col), msg)
    catch
      case _: ColumnNotFoundException => throw RequirementException(col, s"Column $col is missing.")
      case _: ClassCastException      => throw RequirementException(df(col), "Incorrect type. Cannot cast class.")

  /**
    * Raises an exception if predicate function evaluates to false. The series is cast to type `T`.
    *
    * @param col
    *   Column to be checked.
    * @param cond
    *   Predicate function.
    * @param msg
    *   Error message.
    * @return
    *   Requirement.
    * @since 0.1.0
    */
  private def raiseOnErrorTyped[T: ClassTag: Typeable](
      col: String,
      cond: Series[T] => Boolean,
      msg: String = "",
  ): Requirement =
    try if cond(df(col).as[T]) then this else throw RequirementException(df(col), msg)
    catch
      case _: ColumnNotFoundException => throw RequirementException(col, s"Column $col is missing.")
      case _: ClassCastException      => throw RequirementException(df(col), "Incorrect type. Cannot cast class.")
