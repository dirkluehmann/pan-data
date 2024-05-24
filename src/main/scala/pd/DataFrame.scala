/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.*
import pd.implicits.{SeriesAny, SeriesString}
import pd.internal.index.ColIndex.{ColMapOps, toMap}
import pd.internal.index.sort.Sorting
import pd.internal.index.{BaseIndex, ColIndex, IndexOps, UniformIndex}
import pd.internal.series.SeriesData
import pd.internal.utils.StringUtils
import pd.internal.utils.{ConstantIterator, RequireType}
import pd.io.{ReadAdapter, WriteAdapter}
import pd.plot.Plot

import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import scala.annotation.{targetName, unused}
import scala.collection.mutable
import scala.language.{dynamics, implicitConversions, postfixOps}
import scala.math.Ordering.StringOrdering
import scala.reflect.{ClassTag, Typeable}

/**
  * A DataFrame organizes data in columns and rows. The columns are accessed by column names whereas the rows have an
  * integer index.
  *
  * @see
  *   - Creating a DataFrame [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
  *   - Plotting a DataFrame [[https://pan-data.org/scala/basics/plotting-a-dataframe.html]]
  *   - Index Operations [[https://pan-data.org/scala/basics/index-operations.html]]
  *   - Extracting Columns [[https://pan-data.org/scala/basics/extracting-columns.html]]
  *   - DataFrame Columns [[https://pan-data.org/scala/basics/dataframe-columns.html]]
  *   - Extracting Values [[https://pan-data.org/scala/basics/extracting-values.html]]
  *   - Mutable Access [[https://pan-data.org/scala/basics/muteable.html]]
  *   - Joins [[https://pan-data.org/scala/operations/joins.html]]
  *   - Grouping [[https://pan-data.org/scala/operations/grouping.html]]
  * @since 0.1.0
  */
class DataFrame protected (
    private[pd] val colIndex: ColIndex,
    private[pd] val index: BaseIndex,
) extends IndexOps[DataFrame]
    with Dynamic:

  /**
    * Concatenates the DataFrame on the left and the Series on the right hand side.
    *
    * @param series
    *   Series to be concatenated.
    * @return
    *   DataFrame, where the Series is the last column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the right Series must be included in the left DataFrame.
    *   - Data on the right hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the left operand.
    *   - The operator is equivalent to the method [[col]].
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("concat")
  def |[T](series: Series[T]): DataFrame =
    if index.eq(series.index) then new DataFrame(colIndex + (series.name -> series.data), index)
    else
      index.includes(series.index) match
        case 0 => new DataFrame(colIndex + (series.name -> series.data), index)
        case 1 =>
          new DataFrame(colIndex + (series.name -> series.data.maskIndex(series.index)), index)
        case -1 => throw MergeIndexException(series)

  /**
    * Concatenates the DataFrame on the left and the Series on the right hand side.
    *
    * @param namedSeries
    *   Tuple of column name and Series to be concatenated.
    * @return
    *   DataFrame, where the Series is the last column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the right Series must be included in the left DataFrame.
    *   - Data on the right hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the left operand.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("concat")
  def |[T](namedSeries: (String, Series[T])): DataFrame = |(namedSeries._2 as namedSeries._1)

  /**
    * Concatenates the DataFrame on the left and the DataFrame on the right hand side.
    *
    * @param df
    *   DataFrame to be concatenated.
    * @return
    *   DataFrame, where the left DataFrame columns are prior to the columns of the right DataFrame.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the right DataFrame must be included in the left DataFrame.
    *   - Data on the right hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the left operand.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("concat")
  def |(df: DataFrame): DataFrame =
    if index.eq(df.index) then new DataFrame(colIndex ++ df.colIndex, index)
    else
      index.includes(df.index) match
        case 0 => new DataFrame(colIndex ++ df.colIndex, index)
        case 1 =>
          new DataFrame(colIndex ++ df.colIndex.map(_ -> _.maskIndex(df.index)), index)
        case -1 => throw MergeIndexException(df)

  /**
    * Concatenates the Series on the left and the DataFrame on the right hand side.
    *
    * @param series
    *   Series.
    * @return
    *   DataFrame, where the Series is the first column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the left DataFrame must be included in the right Series.
    *   - Data on the left hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the right operand.
    *   - The operators `|` and `::` are equivalent if indices on the left and right side are equal.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("prepend")
  def ::[T](series: Series[T]): DataFrame =
    if index.eq(series.index) then new DataFrame(colIndex.prepend(series.name -> series.data), index)
    else
      index.includes(series.index) match
        case 0 => new DataFrame(colIndex.prepend(series.name -> series.data), index)
        case 1 =>
          new DataFrame(colIndex.prepend(series.name -> series.data.maskIndex(series.index)), index)
        case -1 => throw MergeIndexException(series)

  /**
    * Concatenates the DataFrame on the left and the DataFrame on the right hand side.
    *
    * @param df
    *   DataFrame.
    * @return
    *   DataFrame, where the left DataFrame columns are prior to the columns of the right DataFrame.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The index of the left DataFrame must be included in the right DataFrame.
    *   - Data on the left hand side might be copied if indices are not equivalent.
    *   - The resulting index is equivalent to the right operand.
    *   - The operators `|` and `::` are equivalent if indices on the left and right side are equal.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  @targetName("prepend")
  def ::(df: DataFrame): DataFrame =
    if index.eq(df.index) then new DataFrame(df.colIndex ++ colIndex, index)
    else
      index.includes(df.index) match
        case 0  => new DataFrame(df.colIndex ++ colIndex, index)
        case 1  => new DataFrame(df.colIndex.map(_ -> _.maskIndex(df.index)) ++ colIndex, index)
        case -1 => throw MergeIndexException(this)

  /**
    * Updates a column with all values that are defined in the Series.
    *
    * @param namedSeries
    *   Tuple of column name to be updated and Series.
    * @return
    *   DataFrame with updated column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @throws SeriesCastException
    *   If the data type does not match with the existing column.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - The index of the DataFrame is not altered.
    * @see
    *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  @targetName("update")
  def &[T](namedSeries: (String, Series[T])): DataFrame = &(namedSeries._2 as namedSeries._1)

  /**
    * Updates a column with all values that are defined in the Series. The name of the Series defines the column.
    *
    * @param series
    *   Series.
    * @return
    *   DataFrame with updated column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @throws SeriesCastException
    *   If the data type does not match with the existing column.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - The index of the DataFrame is not altered.
    * @see
    *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  @targetName("updateOperator")
  def &[T](series: Series[T]): DataFrame =
    if contains(series.name) then
      if index.eq(series.index) || index.includes(series.index) != -1 then |(apply(series.name).update(series.asAny))
      else throw MergeIndexException(series)
    else |(series)

  /**
    * Appends a column if not existing in DataFrame. If the column exists, it is not altered.
    *
    * @param name
    *   Column name.
    * @param series
    *   Series.
    * @throws MergeIndexException
    *   If indices are not compatible. If the column exists, no exception is thrown.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - Data of the Series might be copied if indices are not equivalent.
    *   - The index of the DataFrame is not altered.
    * @return
    *   Series with appended column on the right hand side.
    * @since 0.1.0
    */
  def append[T](name: String, series: Series[T]): DataFrame = if contains(name) then this else |(series.as(name))

  /**
    * Appends columns if not existing in DataFrame. If a column exists, it is not altered.
    *
    * @param series
    *   Series to be added. The column names are taken from the Series.
    * @throws MergeIndexException
    *   If indices are not compatible. If the column exists, no exception is thrown.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - Data of the Series might be copied if indices are not equivalent.
    *   - The index of the DataFrame is not altered.
    * @return
    *   Series with appended columns on the right hand side.
    * @since 0.1.0
    */
  def append(series: Series[?]*): DataFrame = cols(series.filterNot(s => contains(s.name))*)

  /**
    * Returns a column as a Series.
    *
    * @param col
    *   Column name.
    * @return
    *   Column as a Series with the name of the column.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  def apply(col: String): Series[Any] = Series[Any](getData(col), index, col)

  /**
    * Returns a value for a columns and a row.
    *
    * @param row
    *   Row.
    * @param col
    *   Column name.
    * @return
    *   Value as Option.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @throws IndexBoundsException
    *   If `row` is not part of the base index.
    * @note
    *   For an optimal performance in a loop, first extract the column as a Series.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def apply[T: RequireType: Typeable: ClassTag](row: Int, col: String): Option[T] = apply(col).as[T].apply(row)

  /**
    * Returns a value for a columns and a row.
    *
    * @param row
    *   Row.
    * @param col
    *   Column name.
    * @return
    *   Value as Option. None if `row` is None.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @throws IndexBoundsException
    *   If `row` is not part of the base index.
    * @note
    *   For an optimal performance in a loop, first extract the column as a Series.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def apply[T: RequireType: Typeable: ClassTag](row: Option[Int], col: String): Option[T] = apply(col).as[T].apply(row)

  /**
    * Returns a value for a columns and a row using a default value for undefined entries.
    *
    * @param row
    *   Row.
    * @param col
    *   Column name.
    * @param default
    *   Default value for undefined values.
    * @return
    *   Value.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @throws IndexBoundsException
    *   If `row` is not part of the base index.
    * @note
    *   For an optimal performance in a loop, first extract the column as a Series.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def apply[T: Typeable: ClassTag](row: Int, col: String, default: => T): T = apply(col).as[T].apply(row, default)

  /**
    * Returns a value for a columns and a row using a default value for undefined entries.
    *
    * @param row
    *   Row.
    * @param col
    *   Column name.
    * @param default
    *   Default value for undefined values.
    * @return
    *   Value. Default value if `row` is None.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @throws IndexBoundsException
    *   If `row` is not part of the base index.
    * @note
    *   For an optimal performance in a loop, first extract the column as a Series.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def apply[T: Typeable: ClassTag](row: Option[Int], col: String, default: => T): T =
    apply(col).as[T].apply(row, default)

  /**
    * Extracts a column and slices the index by intersecting the current index with a `range`.
    *
    * @param range
    *   Range.
    * @param col
    *   Column name.
    * @return
    *   Series with sliced index.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @see
    *   - [[https://pan-data.org/scala/basics/extracting-columns.html]]
    *   - [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(range: Range, col: String): Series[Any] = apply(col).apply(range)

  /**
    * Extracts a column and slices the index by intersecting the current index with a sequence of index positions.
    *
    * @param seq
    *   Sequence of index positions.
    * @param col
    *   Column name.
    * @return
    *   Series with sliced index.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @see
    *   - [[https://pan-data.org/scala/basics/extracting-columns.html]]
    *   - [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(seq: Seq[Int], col: String): Series[Any] = apply(col).apply(seq)

  /**
    * Extracts a column and slices the index by intersecting the current index with an array of index positions.
    *
    * @param array
    *   Array of index positions.
    * @param col
    *   Column name.
    * @return
    *   Series with sliced index.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @see
    *   - [[https://pan-data.org/scala/basics/extracting-columns.html]]
    *   - [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(array: Array[Int], col: String): Series[Any] = apply(col).apply(array)

  /**
    * Extracts a column and slices the index by intersecting the current index with a boolean Series.
    *
    * @param series
    *   Boolean Series as mask, where only index positions kept that are `true`.
    * @param col
    *   Column name.
    * @return
    *   Series with sliced index.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @see
    *   - [[https://pan-data.org/scala/basics/extracting-columns.html]]
    *   - [[https://pan-data.org/scala/basics/index-operations.html]]
    * @since 0.1.0
    */
  def apply(series: Series[Boolean], col: String): Series[Any] = apply(col).apply(series)

  /**
    * Extracts DataFrame with selected columns.
    *
    * @param cols
    *   Column names.
    * @return
    *   DataFrame with selected column.
    * @throws ColumnNotFoundException
    *   If one of the columns is not found.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  @targetName("applyCols")
  def apply(cols: Seq[String]): DataFrame =
    if cols.forall(contains) then DataFrame(cols.map(c => c -> colIndex(c)), index)
    else throw ColumnNotFoundException(this, cols.filterNot(contains))

  /**
    * Appends (or replaces) one columns.
    *
    * @param series
    *   Series to be concatenated.
    * @return
    *   DataFrame, where the Series is appended on the right side.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - An existing column with the same name is replaced.
    *   - The index of the Series must be included in the left DataFrame.
    *   - Series might be copied if indices are not equivalent.
    *   - The index of the DataFrame is not altered.
    *   - This operation is equivalent to the `|` operator.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  def col[T](series: Series[T]): DataFrame = |(series)

  /**
    * Appends (or replaces) a columns.
    *
    * @param col
    *   Name of column to be appended.
    * @param series
    *   Series.
    * @return
    *   DataFrame, where the Series is appended on the right side.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - An existing column with the same name is replaced.
    *   - The index of the Series must be included in the left DataFrame.
    *   - Series might be copied if indices are not equivalent.
    *   - The index of the DataFrame is not altered.
    *   - This operation is equivalent to the `|` operator.
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  def col[T](col: String, series: Series[T]): DataFrame = |(series.as(col))

  /**
    * All columns as array of Series.
    *
    * @return
    *   Array with all columns as Series in defined order. All Series have the same index as the DataFrame.
    * @since 0.1.0
    */
  def columnArray: Array[Series[Any]] = columnIterator.toArray

  /**
    * Iterates over all columns.
    *
    * @return
    *   Iterable over all columns in defined order.
    * @since 0.1.0
    */
  def columnIterator: Iterable[Series[Any]] =
    colIndex.map(c => Series[Any](c._2.asInstanceOf[SeriesData[Any]], index, c._1))

  /**
    * Sequence with column names.
    *
    * @return
    *   Sequence with column names in defined order.
    * @since 0.1.0
    */
  def columns: Seq[String] = colIndex.keys.toSeq

  /**
    * Appends (or replaces) multiple columns.
    *
    * @return
    *   Appender object which appends Series on the right side.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @note
    *   - Existing columns with the same name are replaced by the rightmost column.
    *   - The index of the Series must be included in the left DataFrame.
    *   - Series might be copied if indices are not equivalent.
    *   - The index of the DataFrame is not altered.
    *   - For one column, this operation is equivalent to the `|` operator.
    * @example
    * {{{
    *   df.cols("price" -> Series(10.0, 20.0), "quantity" -> Series(5, 2))
    *   df.cols(Series(10.0, 20.0) as "price", Series(5, 2) as "quantity")
    *   df.cols(Series("price")(10.0, 20.0), Series("quantity")(5, 2))
    *   df.cols(price = Series(10.0, 20.0), quantity = Series(5, 2))
    * }}}
    * @see
    *   - [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  def cols: DataFrame.DataFrameAppender = DataFrame.DataFrameAppender(this)

  /**
    * Determines if a column is in the DataFrame.
    *
    * @param col
    *   Column name.
    * @return
    *   True if DataFrame has the column `col` and false otherwise.
    * @since 0.1.0
    */
  def contains(col: String): Boolean = colIndex.contains(col)

  /**
    * Determines if an object is a DataFrame.
    *
    * @param a
    *   Any object.
    * @return
    *   True if the object is a DataFrame and false otherwise.
    * @since 0.1.0
    */
  def canEqual(a: Any): Boolean = a.isInstanceOf[DataFrame]

  /**
    * Prints the DataFrame as a table with an index column and annotated column types.
    *
    * @param n
    *   The maximal numbers of rows.
    * @param width
    *   The maximal width of a line.
    * @param colWidth
    *   The width of each column.
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  def display(
      n: Int = Settings.printRowLength,
      width: Int = Settings.printWidth,
      colWidth: Int = Settings.printColWidth,
  ): Unit =
    println(
      toString(n = n, width = width, colWidth = colWidth)
    )

  /**
    * Drops all rows with undefined (null) values.
    *
    * @return
    *   DataFrame restricted to rows without undefined values in all columns.
    * @since 0.1.0
    */
  def dropUndefined(): DataFrame = dropUndefined(columns*)

  /**
    * Drops all rows with undefined (null) values with respect to specified columns.
    *
    * @param cols
    *   Columns.
    * @return
    *   DataFrame restricted to rows without undefined values in columns `cols`.
    * @throws ColumnNotFoundException
    *   If one of the columns `cols` does not exist.
    * @since 0.1.0
    */
  def dropUndefined(cols: String*): DataFrame =
    if cols.isEmpty then this
    else
      withIndex(
        cols.tail
          .foldLeft(apply(cols.head).defined.index)((idx, col) => Series[Any](getData(col), idx, col).defined.index)
      )

  /**
    * Determines if the object is a DataFrame and is equivalent. Equivalence implies:
    *   - The same index. The indices are equal if they have the same elements, the same order and the same base index.
    *   - The column names are the same (but may have different order).
    *   - The values in all columns are equal.
    *
    * @param df
    *   DataFrame (or other object) to compare to.
    * @return
    *   True if equal, false otherwise.
    * @since 0.1.0
    */
  override def equals(df: Any): Boolean =
    df match
      case df: DataFrame =>
        df.canEqual(this) && index.equals(df.index) &&
          colIndex.size == df.colIndex.size &&
          columns.forall(c => df.contains(c) && apply(c).equals(df(c)))
      case _ => false

  /**
    * Returns a value for a columns and a row.
    *
    * @param row
    *   Row.
    * @param col
    *   Column name.
    * @return
    *   Value.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @throws NoSuchElementException
    *   If the value is undefined or `row` is not in the index.
    * @note
    *   For an optimal performance in a loop, first extract the column as a Series.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def get[T: RequireType: Typeable: ClassTag](row: Int, col: String): T = apply(col).as[T].get(row)

  /**
    * Returns a value for a columns and a row.
    *
    * @param row
    *   Row.
    * @param col
    *   Column name.
    * @return
    *   Value.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @throws NoSuchElementException
    *   If the value is undefined, `row` is not in the index or `row` is None.
    * @note
    *   For an optimal performance in a loop, first extract the column as a Series.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def get[T: RequireType: Typeable: ClassTag](row: Option[Int], col: String): T = apply(col).as[T].get(row)

  /**
    * Groups the DataFrame by a column.
    *
    * @param col
    *   Column name.
    * @return
    *   Groups.
    * @throws ColumnNotFoundException
    *   If the a column does not exist.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupBy(col: String): DataMap.Groups[Any] = indexBy[Any](col).groups

  /**
    * Groups the DataFrame by columns.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupBy(col1: String, col2: String): DataMap.Groups[(Any, Any)] = indexBy[Any, Any](col1, col2).groups

  /**
    * Groups the DataFrame by columns.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @param col3
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupBy(col1: String, col2: String, col3: String): DataMap.Groups[(Any, Any, Any)] =
    indexBy[Any, Any, Any](col1, col2, col3).groups

  /**
    * Groups the DataFrame by a sequence of columns (of arbitrary length).
    *
    * @param cols
    *   Sequence with column names.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupBy(cols: Seq[String]): DataMap.Groups[Seq[Any]] =
    if cols.isEmpty then throw IllegalOperation.oneColumnGrouping
    val data = cols.map(apply(_).data)
    DataMap[Seq[Any]](dropUndefined(cols*), cols.toArray, ix => data.map(_.get(ix))).groups

  /**
    * Groups the DataFrame by a typed column.
    *
    * @param col
    *   Column name.
    * @return
    *   Groups.
    * @throws ColumnNotFoundException
    *   If the a column does not exist.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByCol[T: Typeable: ClassTag](col: String): DataMap.Groups[T] = indexBy[T](col).groups

  /**
    * Groups the DataFrame by typed columns.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByCol[T1: Typeable: ClassTag, T2: Typeable: ClassTag](col1: String, col2: String): DataMap.Groups[(T1, T2)] =
    indexBy[T1, T2](col1, col2).groups

  /**
    * Groups the DataFrame by typed columns.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @param col3
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByCol[T1: Typeable: ClassTag, T2: Typeable: ClassTag, T3: Typeable: ClassTag](
      col1: String,
      col2: String,
      col3: String,
  ): DataMap.Groups[(T1, T2, T3)] =
    indexBy[T1, T2, T3](col1, col2, col3).groups

  /**
    * Groups the DataFrame by a typed column including undefined values.
    *
    * @param col
    *   Column name.
    * @return
    *   Groups.
    * @throws ColumnNotFoundException
    *   If the a column does not exist.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByColOption[T: Typeable: ClassTag](col: String): DataMap.Groups[Option[T]] = indexByOption[T](col).groups

  /**
    * Groups the DataFrame by typed columns including undefined values.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByColOption[T1: Typeable: ClassTag, T2: Typeable: ClassTag](
      col1: String,
      col2: String,
  ): DataMap.Groups[(Option[T1], Option[T2])] =
    indexByOption[T1, T2](col1, col2).groups

  /**
    * Groups the DataFrame by typed columns including undefined values.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @param col3
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByColOption[T1: Typeable: ClassTag, T2: Typeable: ClassTag, T3: Typeable: ClassTag](
      col1: String,
      col2: String,
      col3: String,
  ): DataMap.Groups[(Option[T1], Option[T2], Option[T3])] =
    indexByOption[T1, T2, T3](col1, col2, col3).groups

  /**
    * Groups the DataFrame by a column including undefined values.
    *
    * @param col
    *   Column name.
    * @return
    *   Groups.
    * @throws ColumnNotFoundException
    *   If the a column does not exist.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByOption(col: String): DataMap.Groups[Option[Any]] = indexByOption[Any](col).groups

  /**
    * Groups the DataFrame by columns including undefined values.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByOption(col1: String, col2: String): DataMap.Groups[(Option[Any], Option[Any])] =
    indexByOption[Any, Any](col1, col2).groups

  /**
    * Groups the DataFrame by columns including undefined values.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @param col3
    *   Column name.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByOption(col1: String, col2: String, col3: String): DataMap.Groups[(Option[Any], Option[Any], Option[Any])] =
    indexByOption[Any, Any, Any](col1, col2, col3).groups

  /**
    * Groups the DataFrame by a sequence of columns (of arbitrary length) including undefined values.
    *
    * @param cols
    *   Sequence with column names.
    * @return
    *   Groups.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groupByOption(cols: Seq[String]): DataMap.Groups[Seq[Option[Any]]] =
    if cols.isEmpty then throw IllegalOperation.oneColumnGrouping
    val data = cols.map(apply(_).data)
    DataMap[Seq[Option[Any]]](this, cols.toArray, ix => data.map(_.apply(ix))).groups

  /**
    * Indexes the DataFrame by a (typed) column.
    *
    * @param col
    *   Column name.
    * @return
    *   DataMap.
    * @throws ColumnNotFoundException
    *   If the a column does not exist.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexBy[T: Typeable: ClassTag](col: String): DataMap[T] =
    val data = apply(col).as[T].data
    DataMap(dropUndefined(col), Array(col), ix => data.get(ix))

  /**
    * Indexes the DataFrame by (typed) columns.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @return
    *   DataMap.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexBy[T1: Typeable: ClassTag, T2: Typeable: ClassTag](col1: String, col2: String): DataMap[(T1, T2)] =
    val data1 = apply(col1).as[T1].data
    val data2 = apply(col2).as[T2].data
    DataMap(dropUndefined(col1, col2), Array(col1, col2), ix => (data1.get(ix), data2.get(ix)))

  /**
    * Indexes the DataFrame by (typed) columns.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @param col3
    *   Column name.
    * @return
    *   DataMap.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexBy[T1: Typeable: ClassTag, T2: Typeable: ClassTag, T3: Typeable: ClassTag](
      col1: String,
      col2: String,
      col3: String,
  ): DataMap[(T1, T2, T3)] =
    val data1 = apply(col1).as[T1].data
    val data2 = apply(col2).as[T2].data
    val data3 = apply(col3).as[T3].data
    DataMap(
      dropUndefined(col1, col2, col3),
      Array(col1, col2, col3),
      ix => (data1.get(ix), data2.get(ix), data3.get(ix)),
    )

  /**
    * Indexes the DataFrame by a sequence of columns (of arbitrary length).
    *
    * @param cols
    *   Sequence with column names.
    * @return
    *   DataMap.
    * @throws IllegalOperation
    *   If sequence of columns is empty.
    * @note
    *   Undefined (null) grouping values are ignored. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexBy(cols: Seq[String]): DataMap[Seq[Any]] =
    if cols.isEmpty then throw IllegalOperation("Indexing, grouping or joining requires at least one column.")
    val data = cols.map(apply(_).data)
    DataMap[Seq[Any]](dropUndefined(cols*), cols.toArray, ix => data.map(_.get(ix)))

  /**
    * Groups the DataFrame by a (typed) column including undefined values.
    *
    * @param col
    *   Column name.
    * @return
    *   DataMap with Option keys.
    * @throws ColumnNotFoundException
    *   If the a column does not exist.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexByOption[T: Typeable: ClassTag](col: String): DataMap[Option[T]] =
    val data = apply(col).as[T].data
    DataMap(this, Array(col), ix => data(ix))

  /**
    * Indexes the DataFrame by (typed) columns including undefined values.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @return
    *   DataMap with Option keys.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexByOption[T1: Typeable: ClassTag, T2: Typeable: ClassTag](
      col1: String,
      col2: String,
  ): DataMap[(Option[T1], Option[T2])] =
    val data1 = apply(col1).as[T1].data
    val data2 = apply(col2).as[T2].data
    DataMap(this, Array(col1, col2), ix => (data1(ix), data2(ix)))

  /**
    * Indexes the DataFrame by (typed) columns including undefined values.
    *
    * @param col1
    *   Column name.
    * @param col2
    *   Column name.
    * @param col3
    *   Column name.
    * @return
    *   DataMap with Option keys.
    * @note
    *   Undefined (null) grouping are assembled in one group. Each Double.NaN value represents an individual group.
    * @see
    *   [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def indexByOption[T1: Typeable: ClassTag, T2: Typeable: ClassTag, T3: Typeable: ClassTag](
      col1: String,
      col2: String,
      col3: String,
  ): DataMap[(Option[T1], Option[T2], Option[T3])] =
    val data1 = apply(col1).as[T1].data
    val data2 = apply(col2).as[T2].data
    val data3 = apply(col3).as[T3].data
    DataMap(this, Array(col1, col2, col3), ix => (data1(ix), data2(ix), data3(ix)))

  /**
    * Iterator over the index.
    *
    * @return
    *   Iterator over the index in the current order.
    * @since 0.1.0
    */
  def indexIterator: Iterator[Int] = index.iterator

  /**
    * Information string describing the DataFrame.
    *
    * @return
    *   Info string.
    * @since 0.1.0
    */
  def info: String =
    s"""DataFrame with columns
       |${colIndex.map(c => s"  ${c._1} ${c._2.typeDescription()}").mkString("\n")}
       |and a ${index.describe}
       |""".stripMargin

  /**
    * Adapter to write the DataFrame to a specific output. Use e.g. {{{import pd.io.parquet.implicits}}} to import the
    * respective format and read a DataFrame via {{{df.io.parquet.write(...)}}}
    *
    * @return
    *   WriteAdapter.
    * @see
    *   [[https://pan-data.org/scala/io/index.html]]
    * @since 0.1.0
    */
  def io: WriteAdapter = WriteAdapter(this)

  /**
    * Performs an inner join equivalent to a SQL inner join with respect to the specified key columns.
    *
    * @param df
    *   DataFrame to be joined.
    * @param cols
    *   Key columns to be joined.
    * @return
    *   The joined DataFrame.
    * @throws IllegalOperation
    *   If not at least one key column is specified.
    * @note
    *   - Columns in `df` are dropped that are not key columns but appear also in the original DataFrame. To keep these
    *     columns in the results, the columns must be renamed first.
    *   - The comparison of two undefined values (null) is false and does not satisfy a join condition.
    * @see
    *   [[https://pan-data.org/scala/operations/joins.html]]
    * @since 0.1.0
    */
  def joinInner(df: DataFrame, cols: String*): DataFrame =
    val left = indexBy(cols)
    val right = df.indexBy(cols)
    val builderLeft = mutable.ArrayBuilder.make[Int]
    val builderRight = mutable.ArrayBuilder.make[Int]
    DataMap.join(left, right, builderLeft, builderRight, inner = true, outer = false)
    DataMap.buildJoin(left, right, builderLeft.result(), builderRight.result())

  /**
    * Performs a left join equivalent to a SQL outer left join with respect to the specified key columns.
    *
    * @param df
    *   DataFrame to be joined.
    * @param cols
    *   Key columns to be joined.
    * @return
    *   The joined DataFrame.
    * @throws IllegalOperation
    *   If not at least one key column is specified.
    * @note
    *   - Columns in `df` are dropped that are not key columns but appear also in the original DataFrame. To keep these
    *     columns in the results, the columns must be renamed first.
    *   - The comparison of two undefined values (null) is false and does not satisfy a join condition.
    * @see
    *   [[https://pan-data.org/scala/operations/joins.html]]
    * @since 0.1.0
    */
  def joinLeft(df: DataFrame, cols: String*): DataFrame =
    val left = indexBy(cols)
    val right = df.indexBy(cols)
    val builderLeft = mutable.ArrayBuilder.make[Int]
    val builderRight = mutable.ArrayBuilder.make[Int]
    DataMap.join(left, right, builderLeft, builderRight, inner = true, outer = true)
    val nullRows = undefinedIndices(cols*)
    builderLeft.addAll(nullRows)
    builderRight.addAll(ConstantIterator(-1, nullRows.length))
    DataMap.buildJoin(left, right, builderLeft.result(), builderRight.result())

  /**
    * Performs an outer join equivalent to a SQL full outer join with respect to the specified key columns.
    *
    * @param df
    *   DataFrame to be joined.
    * @param cols
    *   Key columns to be joined.
    * @return
    *   The joined DataFrame.
    * @throws IllegalOperation
    *   If not at least one key column is specified.
    * @note
    *   - Columns in `df` are dropped that are not key columns but appear also in the original DataFrame. To keep these
    *     columns in the results, the columns must be renamed first.
    *   - The comparison of two undefined values (null) is false and does not satisfy a join condition.
    * @see
    *   [[https://pan-data.org/scala/operations/joins.html]]
    * @since 0.1.0
    */
  def joinOuter(df: DataFrame, cols: String*): DataFrame =
    val left = indexBy(cols)
    val right = df.indexBy(cols)

    def leftJoin =
      val builderLeft = mutable.ArrayBuilder.make[Int]
      val builderRight = mutable.ArrayBuilder.make[Int]
      DataMap.join(left, right, builderLeft, builderRight, inner = true, outer = true)
      val nullRows = undefinedIndices(cols*)
      builderLeft.addAll(nullRows)
      builderRight.addAll(ConstantIterator(-1, nullRows.length))
      DataMap.buildJoin(left, right, builderLeft.result(), builderRight.result())

    def rightJoinWithoutInner =
      val builderLeft = mutable.ArrayBuilder.make[Int]
      val builderRight = mutable.ArrayBuilder.make[Int]
      DataMap.join(right, left, builderRight, builderLeft, inner = false, outer = true)
      val nullRows = df.undefinedIndices(cols*)
      builderLeft.addAll(ConstantIterator(-1, nullRows.length))
      builderRight.addAll(nullRows)
      DataMap.buildRightJoin(left, right, builderLeft.result(), builderRight.result())

    DataFrame.union(leftJoin, rightJoinWithoutInner)

  /**
    * Performs a right join equivalent to a SQL outer right join with respect to the specified key columns.
    *
    * @param df
    *   DataFrame to be joined.
    * @param cols
    *   Key columns to be joined.
    * @return
    *   The joined DataFrame.
    * @throws IllegalOperation
    *   If not at least one key column is specified.
    * @note
    *   - Columns in `df` are dropped that are not key columns but appear also in the original DataFrame. To keep these
    *     columns in the results, the columns must be renamed first.
    *   - The comparison of two undefined values (null) is false and does not satisfy a join condition.
    * @see
    *   [[https://pan-data.org/scala/operations/joins.html]]
    * @since 0.1.0
    */
  def joinRight(df: DataFrame, cols: String*): DataFrame =
    val left = indexBy(cols)
    val right = df.indexBy(cols)
    val builderLeft = mutable.ArrayBuilder.make[Int]
    val builderRight = mutable.ArrayBuilder.make[Int]
    DataMap.join(right, left, builderRight, builderLeft, inner = true, outer = true)
    val nullRows = df.undefinedIndices(cols*)
    builderLeft.addAll(ConstantIterator(-1, nullRows.length))
    builderRight.addAll(nullRows)
    DataMap.buildRightJoin(left, right, builderLeft.result(), builderRight.result())

  /**
    * Merges two DataFrame by index.
    *
    * @param df
    *   DataFrame to merge.
    * @return
    *   DataFrame with all columns of `df` concatenated on the right side.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The order of index positions is only preserved if the indices are equal.
    *   - Use the `join` methods if you intend to join via column values.
    * @since 0.1.0
    */
  def merge(df: DataFrame): DataFrame =
    DataFrame.from(columnIterator.toSeq ++ df.columnIterator.toSeq*)

  /**
    * Merges a Series into a DataFrame by index.
    *
    * @param series
    *   Series to concatenate.
    * @return
    *   DataFrame with all columns of `df` concatenated on the right side.
    * @note
    *   - Columns with the same name are replaced by the rightmost column.
    *   - The order of index positions is only preserved if all indices are equal.
    *   - Use the `join` methods if you intend to join via column values.
    * @since 0.1.0
    */
  def merge(series: Series[?]*): DataFrame = DataFrame.from(columnIterator.toSeq ++ series*)

  /**
    * Number of columns.
    *
    * @return
    *   Number of columns.
    * @since 0.1.0
    */
  def numCols: Int = colIndex.size

  /**
    * Number of rows.
    *
    * @return
    *   Length of the Series, i.e. number of elements in the index.
    * @since 0.1.0
    */
  def numRows: Int = index.length

  /**
    * Number of rows of the underlying data vectors.
    *
    * @return
    *   Number of elements in the base index.
    * @since 0.1.0
    */
  def numRowsBase: Int = index.base.length

  /**
    * Creates a plot.
    *
    * @return
    *   Plot object.
    * @since 0.1.0
    */
  def plot: Plot = Plot(this)

  /**
    * Requirement object which is used to throw exceptions if conditions are not met.
    *
    * @return
    *   Requirement object.
    * @since 0.1.0
    */
  def requires: Requirement = Requirement(this)

  /**
    * Resets the index to a `UniformIndex` with index positions 0 to `numRows - 1` while keeping the order of the
    * elements. If the current index is not a uniform index, the columns are copied into new vectors with the order of
    * the current index.
    *
    * @return
    *   DataFrame with a `UniformIndex`.
    * @see
    *   [[sortIndex]] for sorting the index by index positions.
    * @since 0.1.0
    */
  def resetIndex: DataFrame =
    if index.isInstanceOf[UniformIndex] then this
    else
      val indices = index.toSeqIndex.indices
      val newIndex = UniformIndex(indices.length)
      val newColumns = columnIterator.map(v => (v.name, v.extract(indices).data))
      new DataFrame(newColumns.toSeq, newIndex)

  /**
    * Extracts DataFrame with selected columns.
    *
    * @param cols
    *   Column names.
    * @return
    *   DataFrame with selected column.
    * @throws ColumnNotFoundException
    *   If a column is not found.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  def select(cols: Array[String]): DataFrame = apply(cols.toSeq)

  /**
    * Extracts DataFrame with selected columns.
    *
    * @param cols
    *   Column names.
    * @return
    *   DataFrame with selected column.
    * @throws ColumnNotFoundException
    *   If a column is not found.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  @targetName("selectSeq")
  def select(cols: Seq[String]): DataFrame = apply(cols)

  /**
    * Extracts DataFrame with selected columns.
    *
    * @param cols
    *   Column names.
    * @return
    *   DataFrame with selected column.
    * @throws ColumnNotFoundException
    *   If a column is not found.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  @targetName("selectCols")
  def select(cols: String*): DataFrame = apply(cols)

  /**
    * Selects a column via dot notation.
    *
    * @param col
    *   Column name.
    * @return
    *   Column as a Series with the name of the column.
    * @throws ColumnNotFoundException
    *   If the column is not found.
    * @example
    *   {{{df.myColumn}}}
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-columns.html]]
    * @since 0.1.0
    */
  @unused // dynamic invocation
  def selectDynamic(col: String): Series[Any] = apply(col)

  /**
    * Sorts the DataFrame in ascending order with respect to column `col`.
    *
    * @param col
    *   Column which defines the order.
    * @param ordering
    *   Implicit ordering that must exist for the type `T`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   DataFrame with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted[T](col: String)(implicit ordering: Ordering[T]): DataFrame = sorted(col -> Order.asc)

  /**
    * Sorts the DataFrame in ascending order with respect to tow columns.
    *
    * @param col1
    *   Column which defines the order.
    * @param col2
    *   Column which defines the secondary order.
    * @param ordering1
    *   Implicit ordering that must exist for the type `T1`, i.e. classes must extend the trait [[Ordered]].
    * @param ordering2
    *   Implicit ordering that must exist for the type `T2`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   DataFrame with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted[T1, T2](col1: String, col2: String)(implicit ordering1: Ordering[T1], ordering2: Ordering[T2]): DataFrame =
    sorted[T1, T2](col1 -> Order.asc, col2 -> Order.asc)(ordering1, ordering2)

  /**
    * Sorts the DataFrame in ascending order with respect to three columns.
    *
    * @param col1
    *   Column which defines the order.
    * @param col2
    *   Column which defines the secondary order.
    * @param col3
    *   Column which defines the trinary order.
    * @param ordering1
    *   Implicit ordering that must exist for the type `T1`, i.e. classes must extend the trait [[Ordered]].
    * @param ordering2
    *   Implicit ordering that must exist for the type `T2`, i.e. classes must extend the trait [[Ordered]].
    * @param ordering3
    *   Implicit ordering that must exist for the type `T3`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   DataFrame with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted[T1, T2, T3](col1: String, col2: String, col3: String)(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
      ordering3: Ordering[T3],
  ): DataFrame =
    sorted[T1, T2, T3](col1 -> Order.asc, col2 -> Order.asc, col3 -> Order.asc)(ordering1, ordering2, ordering3)

  /**
    * Sorts the DataFrame with respect to a column.
    *
    * @param key
    *   Tuple `column -> order` with `column` name and `order` for sorting. Possible values are [[Order.asc]],
    *   [[Order.desc]], [[Order.ascNullsFirst]] and [[Order.descNullsFirst]]. Implicit ordering must exist for the type
    *   `T`,
    * i.e. classes must extend the trait [[Ordered]].
    * @return
    *   Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted[T](key: (String, Order))(implicit ordering: Ordering[T]): DataFrame =
    if ordering.isInstanceOf[StringOrdering] then
      withIndex(index.sorted[String](getDataTyped[String](key._1) -> key._2)(SeriesString.DefaultOrder))
    else withIndex(index.sorted[T](getDataTyped[T](key._1) -> key._2))

  /**
    * Sorts the DataFrame with respect to a column.
    *
    * @param key1
    *   Tuple `column -> order` with `column` name and `order` for sorting. Possible values are [[Order.asc]],
    *   [[Order.desc]], [[Order.ascNullsFirst]] and [[Order.descNullsFirst]]. Implicit ordering must exist for the type
    *   `T`,
    * i.e. classes must extend the trait [[Ordered]].
    * @param key2
    *   Secondary sorting key. See `key1` for details.
    * @param ordering1
    *   Implicit ordering that must exist for the type `T1`, i.e. classes must extend the trait [[Ordered]].
    * @param ordering2
    *   Implicit ordering that must exist for the type `T2`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted[T1, T2](key1: (String, Order), key2: (String, Order))(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
  ): DataFrame =
    val o2: Ordering[T2] =
      if ordering2.isInstanceOf[StringOrdering] then SeriesString.DefaultOrder.asInstanceOf[Ordering[T2]] else ordering2
    val s2 = Sorting[T2](getDataTyped[T2](key2._1), key2._2)(o2)
    val o1: Ordering[T1] =
      if ordering1.isInstanceOf[StringOrdering] then SeriesString.DefaultOrder.asInstanceOf[Ordering[T1]] else ordering1
    val s1 = Sorting[T1](getDataTyped[T1](key1._1), key1._2, Some(s2))(o1)
    withIndex(index.sortBy(s1.sort))

  /**
    * Sorts the DataFrame with respect to a column.
    *
    * @param key1
    *   Tuple `column -> order` with `column` name and `order` for sorting. Possible values are [[Order.asc]],
    *   [[Order.desc]], [[Order.ascNullsFirst]] and [[Order.descNullsFirst]]. Implicit ordering must exist for the type
    *   `T`,
    * i.e. classes must extend the trait [[Ordered]].
    * @param key2
    *   Secondary sorting key. See `key1` for details.
    * @param key3
    *   Trinary sorting key. See `key1` for details.
    * @param ordering1
    *   Implicit ordering that must exist for the type `T1`, i.e. classes must extend the trait [[Ordered]].
    * @param ordering2
    *   Implicit ordering that must exist for the type `T2`, i.e. classes must extend the trait [[Ordered]].
    * @param ordering3
    *   Implicit ordering that must exist for the type `T3`, i.e. classes must extend the trait [[Ordered]].
    * @return
    *   Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sorted[T1, T2, T3](key1: (String, Order), key2: (String, Order), key3: (String, Order))(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
      ordering3: Ordering[T3],
  ): DataFrame =
    val o3 =
      if ordering3.isInstanceOf[StringOrdering] then SeriesString.DefaultOrder.asInstanceOf[Ordering[T3]] else ordering3
    val s3 = Sorting[T3](getDataTyped[T3](key3._1), key3._2)(o3)
    val o2 =
      if ordering2.isInstanceOf[StringOrdering] then SeriesString.DefaultOrder.asInstanceOf[Ordering[T2]] else ordering2
    val s2 = Sorting[T2](getDataTyped[T2](key2._1), key2._2, Some(s3))(o2)
    val o1: Ordering[T1] =
      if ordering1.isInstanceOf[StringOrdering] then SeriesString.DefaultOrder.asInstanceOf[Ordering[T1]] else ordering1
    val s1 = Sorting[T1](getDataTyped[T1](key1._1), key1._2, Some(s2))(o1)
    withIndex(index.sortBy(s1.sort))

  /**
    * Sorts the DataFrame in ascending order with respect to columns `cols` for supported types.
    *
    * @param cols
    *   Columns which defines the order.
    * @return
    *   DataFrame with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  @targetName("sortValuesCols")
  def sortValues(cols: String*): DataFrame = sortValues(cols.map(_ -> Order.asc)*)

  /**
    * Sorts the DataFrame with respect to columns `cols` for supported types.
    *
    * @param keys
    *   Tuple `column -> order` with `column` name and `order` for sorting. Possible values are [[Order.asc]],
    *   [[Order.desc]], [[Order.ascNullsFirst]] and [[Order.descNullsFirst]]
    * @return
    *   Series with sorted index.
    * @note
    *   - The sorting algorithm is stable.
    *   - String values are sorted lexicographically ignoring case differences (see String.compareToIgnoreCase).
    * @since 0.1.0
    */
  def sortValues(keys: (String, Order)*): DataFrame =
    if keys.isEmpty then this
    else
      var successor: Option[Sorting] = None
      val sorting = keys.reverse
        .map(f = k =>
          val data = getData(k._1)
          // noinspection TypeCheckCanBeMatch
          val result =
            if data.vector.isInstanceOf[Array[Boolean]] then Sorting(data.as[Boolean], k._2, successor)
            else if data.vector.isInstanceOf[Array[Byte]] then Sorting(data.as[Byte], k._2, successor)
            else if data.vector.isInstanceOf[Array[Char]] then Sorting(data.as[Char], k._2, successor)
            else if data.vector.isInstanceOf[Array[Double]] then Sorting(data.as[Double], k._2, successor)
            else if data.vector.isInstanceOf[Array[Float]] then Sorting(data.as[Float], k._2, successor)
            else if data.vector.isInstanceOf[Array[Int]] then Sorting(data.as[Int], k._2, successor)
            else if data.vector.isInstanceOf[Array[Long]] then Sorting(data.as[Long], k._2, successor)
            else if data.vector.isInstanceOf[Array[Short]] then Sorting(data.as[Short], k._2, successor)
            else if data.vector.isInstanceOf[Array[LocalDate]] then Sorting(data.as[LocalDate], k._2, successor)
            else if data.vector.isInstanceOf[Array[LocalDateTime]] then Sorting(data.as[LocalDateTime], k._2, successor)
            else if data.vector.isInstanceOf[Array[LocalTime]] then Sorting(data.as[LocalTime], k._2, successor)
            else if data.vector.isInstanceOf[Array[String]] then
              Sorting(data.as[String], k._2, successor)(SeriesString.DefaultOrder)
            else if data.vector.isInstanceOf[Array[ZonedDateTime]] then Sorting(data.as[ZonedDateTime], k._2, successor)
            else throw IllegalOperation(apply(k._1), "sortValues")
          successor = Some(result)
          result
        )
        .last

      withIndex(index.sortBy(sorting.sort))

  /**
    * Prints the DataFrame as a table.
    *
    * @param n
    *   The maximal numbers of rows.
    * @param width
    *   The maximal width of a line.
    * @param annotateIndex
    *   If true, the an index column is displayed.
    * @param annotateType
    *   If true, the type for each column in displayed.
    * @param colWidth
    *   The width of each column.
    * @see
    *   [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    * @since 0.1.0
    */
  def show(
      n: Int = Settings.printRowLength,
      width: Int = Settings.printWidth,
      annotateIndex: Boolean = false,
      annotateType: Boolean = false,
      colWidth: Int = Settings.printColWidth,
  ): Unit = println(toString(n, width, annotateIndex, annotateType, colWidth))

  /**
    * Copies a column into an array.
    *
    * @param col
    *   Column name.
    * @return
    *   Array of type `Option[T]` with [[numRows]] elements.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def toArray[T: RequireType: Typeable: ClassTag](col: String): Array[Option[T]] = apply(col).as[T].toArray

  /**
    * Copies a column into an array.
    *
    * @param col
    *   Column name.
    * @return
    *   Array of type `T`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def toFlatArray[T: RequireType: Typeable: ClassTag](col: String): Array[T] = apply(col).as[T].toFlatArray

  /**
    * Copies a column into a List.
    *
    * @param col
    *   Column name.
    * @return
    *   List of type `T`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def toFlatList[T: RequireType: Typeable: ClassTag](col: String): List[T] = apply(col).as[T].toFlatList

  /**
    * Copies a column into a sequence.
    *
    * @param col
    *   Column name.
    * @return
    *   Sequence of type `T`.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def toFlatSeq[T: RequireType: Typeable: ClassTag](col: String): Seq[T] = apply(col).as[T].toFlatSeq

  /**
    * Copies a column into a List.
    *
    * @param col
    *   Column name.
    * @return
    *   List of type `List[T]` with [[numRows]] elements.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def toList[T: RequireType: Typeable: ClassTag](col: String): List[Option[T]] = apply(col).as[T].toList

  /**
    * Copies a column into a sequence.
    *
    * @param col
    *   Column name.
    * @return
    *   Sequence of type `Seq[T]` with [[numRows]] elements.
    * @see
    *   [[https://pan-data.org/scala/basics/extracting-values.html]]
    * @since 0.1.0
    */
  def toSeq[T: RequireType: Typeable: ClassTag](col: String): Seq[Option[T]] = apply(col).as[T].toSeq

  /**
    * Renders the DataFrame as a table using default parameters.
    *
    * @return
    *   Formatted table.
    * @since 0.1.0
    */
  override def toString: String = "\n" + toString()

  /**
    * Renders the DataFrame as a table.
    *
    * @param n
    *   The maximal numbers of rows.
    * @param width
    *   The maximal width of a line.
    * @param annotateIndex
    *   If true, the an index column is displayed.
    * @param annotateType
    *   If true, the type for each column in displayed.
    * @param colWidth
    *   The width of each column.
    * @param indexWidth
    *   The width of the index colum.
    * @return
    *   Formatted table.
    * @since 0.1.0
    */
  def toString(
      n: Int = Settings.printRowLength,
      width: Int = Settings.printWidth,
      annotateIndex: Boolean = true,
      annotateType: Boolean = true,
      colWidth: Int = Settings.printColWidth,
      indexWidth: Int = indexPrintWidth,
  ): String =
    StringUtils.table(
      this,
      n,
      width,
      annotateIndex,
      annotateType,
      colWidth,
      indexWidth,
      indexPrintName,
      indexPrintRepresentation,
    )

  /**
    * Appends row-wise one or multiple DataFrame objects. The method materializes all indices into a uniform index.
    *
    * @param df
    *   DataFrame object to be appended.
    * @return
    *   DataFrame with exactly the same columns.
    * @throws ColumnNotFoundException
    *   If the DataFrame objects to be appended don't have the same columns.
    * @throws SeriesCastException
    *   If the underlying types of the columns do not match.
    * @since 0.1.0
    */
  def union(df: DataFrame*): DataFrame =
    val cols = numCols
    df.foreach(d =>
      if d.numCols != cols then
        throw ColumnNotFoundException(s"Number of columns does not match. Found $cols and ${d.numCols}.")
    )
    DataFrame.fromSeries(
      columnIterator.map(s => s.union(df.map(_.apply(s.name))*)).toSeq,
      false,
    )

  /**
    * Updates a column with all values that are defined in the Series.
    *
    * @param col
    *   Column name to be updated.
    * @param series
    *   Series.
    * @return
    *   DataFrame with updated column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @throws SeriesCastException
    *   If the data type does not match with the existing column.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - The index of the DataFrame is not altered.
    * @see
    *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  def update[T](col: String, series: Series[T]): DataFrame = &(col, series)

  /**
    * Updates a column with all values that are defined in the Series. The name of the Series defines the column.
    *
    * @param series
    *   Series.
    * @return
    *   DataFrame with updated column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @throws SeriesCastException
    *   If the data type does not match with the existing column.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - The index of the DataFrame is not altered.
    * @see
    *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  def update[T](series: Series[T]): DataFrame = &(series)

  /**
    * Updates a columns with all values that are defined in the Series objects. The names of the Series define the
    * column names.
    *
    * @param series
    *   Series.
    * @return
    *   DataFrame with updated column.
    * @throws MergeIndexException
    *   If indices are not compatible.
    * @throws SeriesCastException
    *   If the data types do not match with the existing column.
    * @note
    *   - The index of the Series must be included in the DataFrame.
    *   - The index of the DataFrame is not altered.
    * @see
    *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
    * @since 0.1.0
    */
  def update(series: Series[?]*): DataFrame = series.foldLeft(this)(_ & _)

  /**
    * Counts the number of rows for unique value pairs in key columns (including undefined key values).
    *
    * @param cols
    *   Key columns.
    * @return
    *   DataFrame with the key columns and a "count" column.
    * @since 0.1.0
    */
  def valueCounts(cols: String*): DataFrame = valueCounts(cols.toSeq)

  /**
    * Counts the number of rows for unique value pairs in key columns.
    *
    * @param cols
    *   Key columns.
    * @param countCol
    *   Name of the resulting column.
    * @param dropUndefined
    *   If true, drops rows with undefined values in a key column (the default includes undefined values).
    * @param order
    *   Order of the `countCol` column.
    * @param asFraction
    *   If true, it returns the total fraction as Double for an unique key value relative to the total number of rows
    *   (including undefined values). Otherwise the number of rows is returned as Int column.
    * @return
    *   DataFrame with the key columns and the `countCol` column.
    * @since 0.1.0
    */
  def valueCounts(
      cols: Seq[String],
      countCol: String = "count",
      dropUndefined: Boolean = false,
      order: Order = Order.desc,
      asFraction: Boolean = false,
  ): DataFrame =
    if cols.isEmpty then throw IllegalOperation.oneColumnGrouping
    def groups = if dropUndefined then groupBy(cols) else groupByOption(cols)
    val df = groups.countRows(countCol).collect.sorted[Int](countCol -> order).resetIndex
    if asFraction then df | df(countCol) / numRows.toDouble else df

  // *** PRIVATE ***

  /**
    * SeriesData object of column.
    * @param col
    *   Column name.
    * @return
    *   SeriesData of type Any.
    * @since 0.1.0
    */
  private[pd] inline def getData(col: String): SeriesData[Any] =
    colIndex.getOrElse(col, throw columnNotFound(col)).asInstanceOf[SeriesData[Any]]

  /**
    * SeriesData object of column.
    *
    * @param col
    *   Column name.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  private[pd] inline def getDataTyped[T](col: String): SeriesData[T] =
    colIndex.getOrElse(col, throw columnNotFound(col)).asInstanceOf[SeriesData[T]]

  /**
    * Name for index column to be displayed.
    *
    * @return
    *   Name for index column.
    * @since 0.1.0
    */
  private[pd] def indexPrintName: String = "Index"

  /**
    * Value to be displayed as index.
    *
    * @param ix
    *   Row.
    * @param indexWidth
    *   Width for index column.
    * @return
    *   String representation.
    * @since 0.1.0
    */
  private[pd] def indexPrintRepresentation(ix: Int, indexWidth: Int): String =
    StringUtils.fixedString(ix.toString, indexWidth)

  /**
    * Width of displayed index column.
    *
    * @return
    *   Width.
    * @since 0.1.0
    */
  private[pd] def indexPrintWidth: Int = Settings.printIndexWidth

  /**
    * Throws column ColumnNotFoundException (escaping inline definition).
    *
    * @param col
    *   Column.
    * @return
    *   ColumnNotFoundException.
    * @since 0.1.0
    */
  private[pd] def columnNotFound(col: String): Throwable = ColumnNotFoundException(this, col)

  /**
    * Undefined index positions with respect to the current index.
    *
    * @return
    *   Array with undefined index positions in preserved order.
    * @since 0.1.0
    */
  private[pd] def undefinedIndices: Array[Int] = undefinedIndices(columns*)

  /**
    * Undefined index positions for specified columns with respect to the current index.
    *
    * @param cols
    *   Columns.
    * @return
    *   Array with undefined index positions (unordered).
    * @since 0.1.0
    */
  private[pd] def undefinedIndices(cols: String*): Array[Int] =
    if cols.isEmpty then Array[Int]()
    else
      cols.tail
        .foldLeft[Set[Int]](Series[Any](getData(cols.head), index, cols.head).undefinedIndices.toSet)((set, col) =>
          set ++ Series[Any](getData(col), index, col).undefinedIndices
        )
        .toArray

  /**
    * Creates an instance by applying the index (implements IndexOps interface).
    *
    * @param index
    *   New index.
    * @return
    *   New instance of type DataFrame.
    * @since 0.1.0
    */
  private[pd] def withIndex(index: BaseIndex) = new DataFrame(colIndex, index)

/**
  * @see
  *   [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
  * @since 0.1.0
  */
object DataFrame extends Dynamic:

  /**
    * Creates a DataFrame from column names and collections, e. g.
    * {{{
    *   val df = DataFrame("col1" -> Seq(0, 1, 2), "col2" -> Seq("a", "b", "c"))
    * }}}
    * Use the method `from` to adjust lengths and indices automatically.
    *
    * This method implements dynamic invocations and should not be called directly.
    *
    * @param method
    *   Implements the method `apply` and `from`.
    * @param args
    *   Tuples name -> collection.
    * @return
    *   Created DataFrame.
    * @since 0.1.0
    */
  @unused // dynamic invocation
  def applyDynamic(
      method: String
  )(args: ((String, Series[?]) | Series[?])*): DataFrame =
    if method == "apply" || method == "from" then
      if args.isEmpty then empty
      else
        val series = args.map(_ match
          case e: (String, Series[?]) => e._2 as e._1
          case s: Series[?]           => s
        )
        fromSeries(series, forced = method == "from")
    else throw new UnsupportedOperationException(s"Method `$method` does not exist.")

  /**
    * Creates a DataFrame from keyword arguments, e.g.
    * {{{
    *   val df = DataFrame(col1 = Seq(0, 1, 2), col2 = Seq("a", "b", "c"))
    * }}}
    *
    * This method implements dynamic invocations and should not be called directly.
    *
    * @param method
    *   Implements the method `apply`.
    * @param kwargs
    *   Keyword argument `name = collection`.
    * @return
    *   Created DataFrame.
    * @since 0.1.0
    */
  @unused // dynamic invocation
  def applyDynamicNamed(
      method: String
  )(kwargs: (String, Series[?])*): DataFrame =
    if method == "apply" || method == "from" then
      if kwargs.isEmpty then empty
      else fromSeries(kwargs.map(t => t._2.as(t._1)), forced = method == "from")
    else throw new UnsupportedOperationException(s"Method `$method` does not exist.")

  /**
    * Adapter to read a DataFrame from a source. Use e.g. {{{import pd.io.parquet.implicits}}} to import the respective
    * format and read a DataFrame via {{{DataFrame.io.parquet.read(...)}}}
    *
    * @return
    *   ReadAdapter.
    * @see
    *   - [[https://pan-data.org/scala/basics/creating-a-dataframe.html]]
    *   - [[https://pan-data.org/scala/io/index.html]]
    * @since 0.1.0
    */
  def io: ReadAdapter = ReadAdapter()

  // *** PRIVATE ***

  private[pd] def create(colIndex: ColIndex, index: BaseIndex): DataFrame = new DataFrame(colIndex, index)

  /**
    * An empty DataFrame.
    *
    * @return
    *   Empty DataFrame.
    * @since 0.1.0
    */
  private[pd] def empty: DataFrame = create(ColIndex(), UniformIndex(0))

  /**
    * Internal method that converts a Series into a DataFrame.
    *
    * @param series
    *   Series.
    * @return
    *   Created DataFrame.
    * @since 0.1.0
    */
  private[pd] def fromSeries(series: Series[?]) =
    new DataFrame(ColIndex(series.name -> series.data), series.index)

  /**
    * Internal method that builds a DataFrame from multiple Series.
    *
    * @param columns
    *   Sequence of Series.
    * @param forced
    *   If true, the SeriesData objects are resized if necessary.
    * @return
    *   Created DataFrame.
    * @throws BaseIndexException
    *   if SeriesData lengths are different and forced == false.
    * @note
    *   The order of index positions is only preserved if all indices are equal.
    * @since 0.1.0
    */
  private[pd] def fromSeries(columns: Seq[Series[?]], forced: Boolean): DataFrame =
    if columns.isEmpty then empty
    else
      // First strategy: same indices
      val index = columns.head.index
      val haveSameIndex = columns.length == 1 || columns.tail.forall(s =>
        index.eq(s.index) || (index.hasSameBase(s.index) &&
          index.equals(s.index))
      )
      if haveSameIndex then new DataFrame(columns.map(v => (v.name, v.data)), index)
      // Second strategy: join arbitrary [[UniformIndex]] (only with force = true)
      else if columns.forall(_.index.isInstanceOf[UniformIndex]) then
        if forced then
          val maxLength = columns.map(_.index.length).max
          new DataFrame(columns.map(v => (v.name, v.data.resize(v.index, maxLength))), UniformIndex(maxLength))
        else throw BaseIndexException(columns)
      else
        // Third strategy: union indices if base indices are the same
        val haveSameBase = columns.tail.forall(s => index.hasSameBase(s.index))
        if haveSameBase || forced then
          if haveSameBase then
            val newIndex = BaseIndex.unionSameBase(columns.map(_.index))
            new DataFrame(columns.map(v => (v.name, v.data.maskIndex(v.index))), newIndex)
          else
            // Forth strategy: union indices regardless (only with force = true)
            val newIndex = BaseIndex.union(columns.map(_.index))
            new DataFrame(columns.map(v => (v.name, v.data.maskIndexResize(v.index, newIndex.max + 1))), newIndex)
        else throw BaseIndexException(columns)

  /**
    * Appends row-wise one or multiple DataFrame objects to the first one. The method materializes all indices into a
    * uniform index.
    *
    * @param df
    *   DataFrame object to be appended.
    * @return
    *   DataFrame with exactly the same columns.
    * @throws ColumnNotFoundException
    *   If the DataFrame objects to be appended don't have the same columns.
    * @throws IllegalOperation
    *   If no DataFrame is passed.
    * @throws SeriesCastException
    *   If the underlying types of the columns do not match.
    * @since 0.1.0
    */
  def union(df: DataFrame*): DataFrame =
    if df.isEmpty then throw IllegalOperation("Union expects at least one element.")
    else df.head.union(df.tail*)

  // *** INNER CLASSES ***

  class DataFrameAppender(df: DataFrame) extends Dynamic:

    /**
      * Appends (or replaces) multiple columns. Use `df.cols(...)` for calling this method.
      *
      * @return
      *   Appender object which appends Series on the right side.
      * @throws MergeIndexException
      *   If indices are not compatible.
      * @note
      *   - Existing columns with the same name are replaced by the rightmost column.
      *   - The index of the Series must be included in the left DataFrame.
      *   - Series might be copied if indices are not equivalent.
      *   - The index of the DataFrame is not altered.
      *   - For one column, this operation is equivalent to the `|` operator.
      * @example
      * {{{
      *   df.cols("price" -> Series(10.0, 20.0), "quantity" -> Series(5, 2))
      *   df.cols(Series(10.0, 20.0) as "price", Series(5, 2) as "quantity")
      *   df.cols(Series("price")(10.0, 20.0), Series("quantity")(5, 2))
      * }}}
      * @see
      *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
      * @since 0.1.0
      */
    @unused // dynamic invocation
    def applyDynamic(method: String)(args: ((String, Series[?]) | Series[?])*): DataFrame =
      if method == "apply" then
        args.foldLeft(df)((df, b) =>
          b match
            case e: (String, Series[?]) => df.col(e._1, e._2)
            case s: Series[?]           => df | s
        )
      else throw new UnsupportedOperationException(s"Method `$method` does not exist.")

    /**
      * Appends (or replaces) multiple columns. Use `df.cols(...)` for calling this method.
      *
      * @return
      *   Appender object which appends Series on the right side.
      * @throws MergeIndexException
      *   If indices are not compatible.
      * @note
      *   - Existing columns with the same name are replaced by the rightmost column.
      *   - The index of the Series must be included in the left DataFrame.
      *   - Series might be copied if indices are not equivalent.
      *   - The index of the DataFrame is not altered.
      *   - For one column, this operation is equivalent to the `|` operator.
      * @example
      * {{{
      *   df.cols(price = Series(10.0, 20.0), quantity = Series(5, 2))
      * }}}
      * @see
      *   [[https://pan-data.org/scala/basics/dataframe-columns.html]]
      * @since 0.1.0
      */
    @unused // dynamic invocation
    def applyDynamicNamed(method: String)(kwargs: (String, Series[?])*): DataFrame =
      if method == "apply" then kwargs.foldLeft(df)((a, b) => a.col(b._1, b._2))
      else throw new UnsupportedOperationException(s"Method `$method` does not exist.")
