/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd

import pd.exception.{IllegalOperation, MapToNullException}
import pd.implicits.SeriesAny
import pd.internal.index.{SeqIndex, UniformIndex}
import pd.internal.series.SeriesData
import pd.internal.utils.StringUtils.fixedString
import pd.{Series, math}

import scala.annotation.targetName
import scala.collection.mutable
import scala.language.{dynamics, implicitConversions}
import scala.reflect.{ClassTag, Typeable}

/**
  * A DataMap is a DataFrame with a value-to-index mapping allowing fast hash-based access and grouped operations.
  *
  * @see
  *   - DataMap [[https://pan-data.org/scala/operations/datamap.html]]
  *   - Grouping [[https://pan-data.org/scala/operations/grouping.html]]
  *
  * @note
  *   - Column operations return a DataMap[K] as a subtype of DataFrame.
  *   - You can drop the group columns but altering or replacing a group column results in an IllegalOperation exception
  *     (for methods which return the type `DataMap[K]`).
  *   - Note that index changing operations return a DataFrame since the mapping is no longer applicable to the
  *     resulting DataFrame.
  * @since 0.1.0
  */
class DataMap[K](private val df: DataFrame, private val groupCols: Array[String], private val map: Map[K, Array[Int]])
    extends DataFrame(df.colIndex, df.index):

  /**
    * Drops the group columns. The mapping of the values in the group columns to the rows is preserved.
    *
    * @return
    *   DataMap without group columns.
    * @since 0.1.0
    */
  def dropGroupCols: DataMap[K] =
    DataMap(super.apply(columns.filterNot(c => groupCols.contains(c))), Array[String](), map)

  /**
    * Create groups from the DataMap which allow performing group-wise operations such as aggregation.
    *
    * @return
    *   Groups object.
    * @see
    *   - Grouping [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  def groups: DataMap.Groups[K] = DataMap.Groups(this)

  /**
    * Selects a key in the DataMap.
    *
    * @param k
    *   Key value to match.
    * @return
    *   DataFrame for the selected key. If the key is not found, this DataFrame is empty.
    * @note
    *   - Since only the index is set and the keys are hashed, this method is highly performant.
    *   - Note that the resulting DataFrame can have more than one row.
    * @since 0.1.0
    */
  def key(k: K): DataFrame = map.get(k) match
    case Some(idx) => df.withIndex(SeqIndex.unchecked(df.index.base, idx))
    case None      => df(Array[Int]())

  /**
    * Keys in the map.
    *
    * @return
    *   Iterable of keys.
    * @since 0.1.0
    */
  def keys: Iterable[K] = map.keys

  /**
    * Sorts the DataFame with respect to the size of the groups staring with the largest group.
    *
    * @return
    *   DataMap with sorted index.
    * @since 0.1.0
    */
  def sortByGroupSize: DataMap[K] =
    val builder = mutable.ArrayBuilder.make[Int]
    builder.sizeHint(numRows)
    map.values.toSeq.sortBy(-_.length).foreach(builder.addAll(_))
    new DataMap(withIndex(SeqIndex(index.base, builder.result)), groupCols, map)

  // *** OVERRIDDEN MEMBERS FROM DATAFRAME ***

  @targetName("concat")
  override def |[T](series: Series[T]): DataMap[K] =
    checkForGroupColumn(series)
    withDataFrame(super.|(series))

  @targetName("concat")
  override def |[T](namedSeries: (String, Series[T])): DataMap[K] =
    checkForGroupColumn(namedSeries._2)
    withDataFrame(super.|(namedSeries))

  @targetName("concat")
  override def |(df: DataFrame): DataMap[K] =
    checkForGroupColumn(df)
    withDataFrame(super.|(df))

  @targetName("prepend")
  override def ::[T](series: Series[T]): DataMap[K] =
    checkForGroupColumn(series)
    withDataFrame(super.::(series))

  @targetName("prepend")
  override def ::(df: DataFrame): DataMap[K] =
    checkForGroupColumn(df)
    withDataFrame(super.::(df))

  @targetName("update")
  override def &[T](namedSeries: (String, Series[T])): DataMap[K] =
    checkForGroupColumn(namedSeries._2)
    withDataFrame(super.&(namedSeries))

  @targetName("updateOperator")
  override def &[T](series: Series[T]): DataMap[K] =
    checkForGroupColumn(series)
    withDataFrame(super.&(series))

  @targetName("applyCols")
  override def apply(cols: Seq[String]): DataMap[K] =
    withDataFrame(super.apply(cols))

  override def append[T](name: String, series: Series[T]): DataMap[K] =
    checkForGroupColumn(name)
    withDataFrame(super.append(name, series))

  override def append(series: Series[?]*): DataMap[K] =
    checkForGroupColumn(series*)
    withDataFrame(super.append(series*))

  override def col[T](series: Series[T]): DataMap[K] =
    checkForGroupColumn(series)
    withDataFrame(super.col(series))

  override def col[T](col: String, series: Series[T]): DataMap[K] =
    checkForGroupColumn(col)
    withDataFrame(super.col(col, series))

  override def sorted[T](col: String)(implicit ordering: Ordering[T]): DataMap[K] =
    withDataFrame(super.sorted(col))

  override def sorted[T1, T2](col1: String, col2: String)(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
  ): DataFrame =
    withDataFrame(super.sorted[T1, T2](col1, col2)(ordering1, ordering2))

  override def sorted[T1, T2, T3](col1: String, col2: String, col3: String)(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
      ordering3: Ordering[T3],
  ): DataFrame =
    withDataFrame(super.sorted[T1, T2, T3](col1, col2, col3)(ordering1, ordering2, ordering3))

  override def sorted[T](key: (String, Order))(implicit ordering: Ordering[T]): DataMap[K] =
    withDataFrame(super.sorted(key))

  override def sorted[T1, T2](key1: (String, Order), key2: (String, Order))(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
  ): DataFrame =
    withDataFrame(super.sorted[T1, T2](key1, key2)(ordering1, ordering2))

  override def sorted[T1, T2, T3](key1: (String, Order), key2: (String, Order), key3: (String, Order))(implicit
      ordering1: Ordering[T1],
      ordering2: Ordering[T2],
      ordering3: Ordering[T3],
  ): DataFrame =
    withDataFrame(super.sorted[T1, T2, T3](key1, key2, key3)(ordering1, ordering2, ordering3))

  @targetName("sortValuesCols")
  override def sortValues(cols: String*): DataMap[K] =
    withDataFrame(super.sortValues(cols*))

  override def sortValues(keys: (String, Order)*): DataMap[K] =
    withDataFrame(super.sortValues(keys*))

  override def update[T](col: String, series: Series[T]): DataMap[K] =
    checkForGroupColumn(col)
    withDataFrame(super.update(col, series))

  override def update[T](series: Series[T]): DataMap[K] =
    checkForGroupColumn(series)
    withDataFrame(super.update(series))

  override def update(series: Series[?]*): DataMap[K] =
    checkForGroupColumn(series*)
    withDataFrame(super.update(series*))

  // *** PRIVATE ***

  private[pd] def checkForGroupColumn(df: DataFrame): Unit =
    df.columns.foreach(checkForGroupColumn)

  private[pd] def checkForGroupColumn(name: String): Unit =
    if groupCols.contains(name) then
      throw IllegalOperation(s"Cannot alter or replace a group column '$name' in a DataMap.")

  private[pd] def checkForGroupColumn(series: Series[?]*): Unit =
    series.foreach(s => checkForGroupColumn(s.name))

  override private[pd] def indexPrintName: String = if groupCols.isEmpty then "Index: Key" else s"Index"

  override private[pd] def indexPrintRepresentation(ix: Int, indexWidth: Int): String =
    def search(ix: Int): String =
      val it = map.iterator
      var found: Option[K] = None
      while it.hasNext && found.isEmpty do
        val e = it.next()
        if e._2.contains(ix) then found = Some(e._1)
      if found.isEmpty then "" else found.get.toString

    if groupCols.isEmpty then
      fixedString(
        s"$ix: " + search(ix),
        indexWidth,
        leftAlign = true,
      )
    else
      fixedString(
        s"$ix: " + groupCols
          .map(col => df[Any](ix, col).orElse(Some("null")))
          .map(_.get.toString)
          .mkString("(", ",", ")"),
        indexWidth,
        leftAlign = true,
      )

  override private[pd] def indexPrintWidth: Int = Settings.printIndexWidthMapped

  /**
    * Recreates a DataMap with a different DataFrame. The DataFrame must have the same index.
    *
    * @param df
    *   DataFrame with the same index.
    * @return
    *   DataMap.
    * @since 0.1.0
    */
  private[pd] def withDataFrame(df: DataFrame): DataMap[K] = DataMap(df, groupCols, map)

/**
  * @see
  *   DataMap [[https://pan-data.org/scala/operations/datamap.html]]
  * @since 0.1.0
  */
object DataMap:
  /**
    * Creates a DataMap from a DataFrame.
    *
    * @param df
    *   DataFrame.
    * @param groupCols
    *   Groups columns.
    * @param f
    *   Mapping from index to key value.
    * @return
    *   DataMap.
    * @since 0.1.0
    */
  private[pd] def apply[K](df: DataFrame, groupCols: Array[String], f: Int => K): DataMap[K] =
    new DataMap[K](df, groupCols, df.index.toSeqIndex.indices.groupBy(f))

  /**
    * Creates a DataMap from a DataFrame with mapping.
    *
    * @param df
    *   DataFrame.
    * @param groupCols
    *   Group columns.
    * @param map
    *   Mapping key value to index (usually from a DataMap).
    * @return
    *   DataMap.
    * @since 0.1.0
    */
  private[pd] def apply[K](df: DataFrame, groupCols: Array[String], map: Map[K, Array[Int]]) =
    new DataMap[K](df, groupCols, map)

  /**
    * Builds a join taking the group column from the left DataMap `dm1`.
    *
    * @param dm1
    *   Left DataMap.
    * @param dm2
    *   Right DataMap.
    * @param index1
    *   Indices to be taken from the left DataMap.
    * @param index2
    *   Indices to be taken from the right DataMap.
    * @return
    *   Joined DataFrame.
    * @since 0.1.0
    */
  private[pd] def buildJoin[K](dm1: DataMap[K], dm2: DataMap[K], index1: Array[Int], index2: Array[Int]): DataFrame =
    val leftCols = dm1.columns
    val left: Seq[Series[Any]] = dm1.columnIterator.map(_.extract(index1)).toSeq
    val right: Seq[Series[Any]] = dm2.columnIterator
      .filterNot(s => leftCols.contains(s.name))
      .map(_.extract(index2))
      .toSeq
    DataFrame(left ++ right*)

  /**
    * Builds a join taking the group column from the right DataMap `dm2`.
    *
    * @param dm1
    *   Left DataMap.
    * @param dm2
    *   Right DataMap.
    * @param index1
    *   Indices to be taken from the left DataMap.
    * @param index2
    *   Indices to be taken from the right DataMap.
    * @return
    *   Joined DataFrame.
    * @since 0.1.0
    */
  private[pd] def buildRightJoin[K](
      dm1: DataMap[K],
      dm2: DataMap[K],
      index1: Array[Int],
      index2: Array[Int],
  ): DataFrame =
    val leftCols = dm1.columns.filterNot(dm1.groupCols.contains(_))
    val left: Seq[Series[Any]] = dm1.columnIterator
      .filterNot(s => dm1.groupCols.contains(s.name))
      .map(_.extract(index1))
      .toSeq
    val right: Seq[Series[Any]] = dm2.columnIterator
      .filterNot(s => leftCols.contains(s.name))
      .map(_.extract(index2))
      .toSeq
    DataFrame(left ++ right*).apply(dm1.columns ++ dm2.columns.filterNot(dm1.columns.contains(_)))

  /**
    * Computes indices for the join of two DataMap objects.
    *
    * @param dm1
    *   Left DataMap.
    * @param dm2
    *   Right DataMap.
    * @param builder1
    *   Mutable container for left indices.
    * @param builder2
    *   Mutable container for right indices.
    * @param inner
    *   If true, adds indices where left and right group columns match.
    * @param outer
    *   If true, adds outer columns that are in the left DataMap only.
    * @since 0.1.0
    */
  private[pd] def join[K](
      dm1: DataMap[K],
      dm2: DataMap[K],
      builder1: mutable.ArrayBuilder[Int],
      builder2: mutable.ArrayBuilder[Int],
      inner: Boolean,
      outer: Boolean,
  ): Unit =
    val it = dm1.map.iterator
    while it.hasNext do
      val next = it.next
      val idx1 = next._2
      val idx2Option = dm2.map.get(next._1)
      if inner && idx2Option.isDefined then
        var i1 = 0
        var i2 = 0
        val idx2 = idx2Option.get
        val l1 = idx1.length
        val l2 = idx2.length
        while i1 < l1 do
          i2 = 0
          builder2.addAll(idx2)
          val ix1 = idx1(i1)
          while i2 < l2 do
            builder1.addOne(ix1)
            i2 = i2 + 1
          i1 = i1 + 1
      if outer && idx2Option.isEmpty then
        builder1.addAll(idx1)
        val l1 = idx1.length
        var i1 = 0
        while i1 < l1 do
          builder2.addOne(-1)
          i1 = i1 + 1

  // ** INNER CLASSES **

  /**
    * Aggregation groups.
    *
    * @param dm
    *   DataMap.
    * @param aggregates
    *   Sequence of aggregations.
    * @see
    *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  class Groups[K](
      private val dm: DataMap[K],
      private val aggregates: Seq[Series[Any]] = Seq[Series[Any]](),
  ):

    /**
      * Aggregation: Performs a group-wise aggregation of one column.
      *
      * @param col
      *   Column to be aggregated.
      * @param op
      *   Aggregation operation.
      * @param newCol
      *   New column name or empty string for using `col` as output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def agg[T: Typeable: ClassTag, R: ClassTag](
        col: String,
        op: Series[T] => R,
        newCol: String = "",
    ): Groups[K] =
      agg(dm.apply(col).as[T], op, newCol)

    /**
      * Aggregation: Performs a group-wise aggregation of two column.
      *
      * @param col1
      *   First column to be aggregated.
      * @param col2
      *   Second column to be aggregated.
      * @param op
      *   Aggregation operation.
      * @param newCol
      *   New column name.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def agg[T1: Typeable: ClassTag, T2: Typeable: ClassTag, R: ClassTag](
        col1: String,
        col2: String,
        op: (Series[T1], Series[T2]) => R,
        newCol: String,
    ): Groups[K] =
      agg(dm.apply(col1).as[T1], dm.apply(col2).as[T2], op, newCol)

    /**
      * Collects all aggregation results into a DataFrame.
      *
      * @return
      *   DataFrame with group columns and resulting columns for each group, where the number of rows equals the number
      *   of groups.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def collect: DataFrame =
      val indices = firstIndices
      if dm.groupCols.isEmpty then DataFrame(aggregates*)
      else
        val k: Seq[Series[Any]] = dm.groupCols.map(dm.apply(_).extract(indices)).toSeq
        aggregates.foldLeft(DataFrame(k*))(_ | _)

    /**
      * Collects all aggregation results into a DataMap.
      *
      * @return
      *   DataMap with group columns and aggregated values for each group, where the number of rows equals the number of
      *   groups (each key corresponds to one row).
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def collectAsMap: DataMap[K] =
      DataMap(collect, dm.groupCols, dm.map.keys.zipWithIndex.map(e => (e._1, Array(e._2))).toMap)

    /**
      * Collects all aggregation results into a DataMap.
      *
      * @param dropGroupCols
      *   If true, the group columns are dropped.
      * @return
      *   DataMap with group columns and aggregated values for each group, where the number of rows equals the number of
      *   groups (each key corresponds to one row).
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def collectAsMap(dropGroupCols: Boolean = false): DataMap[K] =
      if dropGroupCols then
        if aggregates.isEmpty then throw IllegalOperation("Cannot create DataMap since no aggregations are defined.")
        DataMap(DataFrame(aggregates*), Array[String](), dm.map.keys.zipWithIndex.map(e => (e._1, Array(e._2))).toMap)
      else collectAsMap

    /**
      * Aggregation: Counts defined (non-null) values of a column per a group.
      *
      * @param col
      *   Column to be aggregated.
      * @param newCol
      *   New column name or empty string for using `col` as output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column.
      * @see
      *   [[Series.count]]
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def count(col: String, newCol: String = ""): Groups[K] =
      agg(dm(col), _.count, newCol)

    /**
      * Aggregation: Counts the number of rows in each group.
      *
      * @return
      *   Groups object with aggregation added.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def countRows: Groups[K] = countRows()

    /**
      * Aggregation: Counts the number of rows in each group.
      *
      * @param newCol
      *   New column name or empty string for using "count" as output column.
      * @param asFraction
      *   If true, returns the number of rows divided by the total number of rows as a Double.
      * @return
      *   Groups object with aggregation added.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def countRows(newCol: String = "count", asFraction: Boolean = false): Groups[K] =
      var i = 0
      val it = dm.map.values.iterator
      if asFraction then
        val array = new Array[Double](dm.map.size)
        val totalRows = dm.numRows.toDouble
        while it.hasNext do
          val idx = it.next
          array(i) = idx.length / totalRows
          i += 1
        append(Series[Double](SeriesData(array, null), UniformIndex(dm.map.size), newCol))
      else
        val array = new Array[Int](dm.map.size)
        while it.hasNext do
          val idx = it.next
          array(i) = idx.length
          i += 1
        append(Series[Int](SeriesData(array, null), UniformIndex(dm.map.size), newCol))

    /**
      * Aggregation: First value of a column per a group.
      *
      * @param col
      *   Column to be aggregated.
      * @param newCol
      *   New column name or empty string for using `col` as output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def first[T: ClassTag: Typeable](col: String, newCol: String = ""): Groups[K] =
      aggOption(dm(col).as[T], (s: Series[T]) => s.headOption, newCol)

    /**
      * Aggregation: Performs a group-wise minimum for numeric data types (Boolean, Double, Int).
      *
      * @param col
      *   Column to be aggregated.
      * @param newCol
      *   New column name or empty string for using `col` as output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column or if operation is applied to a non-numeric column.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def min(col: String, newCol: String = ""): Groups[K] =
      val s = dm(col)
      if s.isBoolean then agg[Boolean, Boolean](s.asBoolean, _.min, newCol)
      else if s.isDouble then agg[Double, Double](s.toDouble, _.min, newCol)
      else if s.isInt then agg[Int, Int](s.toInt, _.min, newCol)
      else throw IllegalOperation(s, "min")

    /**
      * Aggregation: Performs a group-wise maximum for numeric data types (Boolean, Double, Int).
      *
      * @param col
      *   Column to be aggregated.
      * @param newCol
      *   New column name or empty string for using `col` as output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column or if operation is applied to a non-numeric column.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def max(col: String, newCol: String = ""): Groups[K] =
      val s = dm(col)
      if s.isBoolean then agg[Boolean, Boolean](s.asBoolean, _.max, newCol)
      else if s.isDouble then agg[Double, Double](s.toDouble, _.max, newCol)
      else if s.isInt then agg[Int, Int](s.toInt, _.max, newCol)
      else throw IllegalOperation(s, "max")

    /**
      * Creates a pivot DataFrame where the aggregations columns are distributed in columns per pivot value.
      *
      * @param col
      *   Group column which is divided into n columns per aggregation, where `n` is the number of distinct values in
      *   the column.
      * @param names
      *   Generator for pivot column names taking a pivot value for column `col` and the aggregation name.
      * @return
      *   Pivot table with group columns (without `col`) and pivoted aggregation columns.
      * @throws IllegalOperation
      *   If `col` is not a group column, there is no group column other than `col` or if a value in `col` matches with
      *   the name of one of the other group columns.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def pivot(
        col: String,
        mapping: Series[Any] => Series[?] = null,
        names: (Any, String) => String = (value: Any, aggCol: String) =>
          if aggregates.length > 1 then s"$value:$aggCol" else s"$value",
    ): DataFrame =
      val firstGroupIndices = firstIndices
      if (!dm.groupCols.contains(col)) throw IllegalOperation(s"Column 'col' must be a group column.")
      if (dm.groupCols.length < 2) throw IllegalOperation(s"Pivot requires grouping by at least two group columns.")
      val df = DataFrame(dm.groupCols.map(dm.apply(_).extract(firstGroupIndices))*)

      val pivotSeries = df(col)
      val values = pivotSeries.distinct
      // noinspection ScalaRedundantCast
      val sortedValues =
        try values.sortValues.resetIndex.map(_.asInstanceOf[Any]).data.vector
        catch case _: IllegalOperation => values.resetIndex.data.vector

      val pivotedIndices = sortedValues.map(v => (v, pivotSeries == v))
      val pivotedAgg = aggregates.flatMap(s => pivotedIndices.map(idx => s(idx._2) as names(idx._1, s.name)))
      val groupedDf = pivotedAgg.foldLeft(df)(_ | _)
      val nonPivotGroupCols = dm.groupCols.filterNot(_ == col)
      val groupedDm = groupedDf.groupBy(nonPivotGroupCols.toSeq)
      pivotedAgg
        .filter(s => nonPivotGroupCols.contains(s.name))
        .foreach(s =>
          throw IllegalOperation(
            s"Pivoted values in column `$col` cannot equal a group column name. Found value: '${s.name}'. " +
              s"Group columns: ${dm.groupCols.mkString(", ")}."
          )
        )
      val finalDf = pivotedAgg.foldLeft(groupedDm)((groups: DataMap.Groups[?], s) => groups.sum(s.name)).collect
      if mapping == null then finalDf else pivotedAgg.foldLeft(finalDf)((df, s) => df | mapping(df(s.name)))

    /**
      * Aggregation: Performs a group-wise sum for numeric data types (Boolean, Double, Int).
      *
      * @param col
      *   Column to be aggregated.
      * @param newCol
      *   New column name or empty string for using `col` as output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column or if operation is applied to a non-numeric column.
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def sum(col: String, newCol: String = ""): Groups[K] =
      val s = dm(col)
      if s.isBoolean then agg[Boolean, Int](s.asBoolean, _.sum, newCol)
      else if s.isDouble then agg[Double, Double](s.toDouble, _.sum, newCol)
      else if s.isInt then agg[Int, Int](s.toInt, _.sum, newCol)
      else throw IllegalOperation(s, "sum")

    /**
      * Transforms all aggregation results into a the original DataFrame. Aggregation results are stored in each row of
      * a group.
      *
      * @return
      *   DataMap with group columns and aggregated values for each group, where the number of rows equals the number of
      *   groups (each key corresponds to one row).
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def transform: DataFrame = aggregates.foldLeft[DataFrame](dm)(_ | expand(_))

    /**
      * Transforms all aggregation results into a the original DataMap which is a subtype of the DataFrame. Aggregation
      * results are stored in each row of a group.
      *
      * @return
      *   DataMap with group columns and aggregated values for each group, where the number of rows equals the number of
      *   groups (each key corresponds to one row).
      * @see
      *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
      * @since 0.1.0
      */
    def transformAsMap: DataMap[K] =
      DataMap(aggregates.foldLeft[DataFrame](dm)(_ | expand(_)), dm.groupCols, dm.map)

    // *** GROUPS PRIVATE ***

    /**
      * Performs the aggregation on sub groups.
      *
      * @param series
      *   Series to be aggregated.
      * @param op
      *   Aggregation operation.
      * @param newCol
      *   New column name or empty string for using `col` as the output column.
      * @return
      *   Groups object with aggregation added.
      * @throws IllegalOperation
      *   If a group column is used a the result column.
      * @throws MapToNullException
      *   If a result is null.
      * @since 0.1.0
      */
    private[pd] def agg[T, R: ClassTag](
        series: Series[T],
        op: Series[T] => R,
        newCol: String,
    ): Groups[K] =
      var i = 0
      val it = dm.map.values.iterator
      val array = new Array[R](dm.map.size)

      while it.hasNext do
        val idx = it.next
        val v = op(series.extract(idx))
        if v == null then throw MapToNullException()
        array(i) = v
        i += 1

      append(
        Series[R](SeriesData(array, null), UniformIndex(dm.map.size), if newCol.isEmpty then series.name else newCol)
      )

    /**
      * Performs the aggregation on sub groups.
      *
      * @param series
      *   Series to be aggregated.
      * @param op
      *   Aggregation operation.
      * @param newCol
      *   New column name or empty string for using `col` as the output column.
      * @return
      *   Groups object with aggregation added.
      * @throws MapToNullException
      *   If a result is null.
      * @since 0.1.0
      */
    private[pd] def aggOption[T, R: ClassTag](
        series: Series[T],
        op: Series[T] => Option[R],
        newCol: String,
    ): Groups[K] =
      var i = 0
      val it = dm.map.values.iterator
      val array = new Array[R](dm.map.size)
      val mask = Array.fill[Boolean](dm.map.size)(true)
      var containsNull = false

      while it.hasNext do
        val idx = it.next
        val v = op(series.extract(idx))
        if v == null then throw MapToNullException()
        if v.isDefined then array(i) = v.get
        else
          mask(i) = false
          containsNull = true
        i += 1

      append(
        Series[R](
          SeriesData(array, if containsNull then mask else null),
          UniformIndex(dm.map.size),
          if newCol.isEmpty then series.name else newCol,
        )
      )

    /**
      * Performs the aggregation with two columns on sub groups.
      *
      * @param series1
      *   Series to be aggregated.
      * @param series2
      *   Series to be aggregated.
      * @param op
      *   Aggregation operation.
      * @param newCol
      *   New column name or empty string for using `col` as the output column.
      * @return
      *   Groups object with aggregation added.
      * @since 0.1.0
      */
    private[pd] def agg[T1, T2, R: ClassTag](
        series1: Series[T1],
        series2: Series[T2],
        op: (Series[T1], Series[T2]) => R,
        newCol: String,
    ): Groups[K] =
      var i = 0
      val it = dm.map.values.iterator
      val array = new Array[R](dm.map.size)

      while it.hasNext do
        val idx = it.next
        val v = op(series1.extract(idx), series2.extract(idx))
        if v == null then throw MapToNullException()
        array(i) = v
        i += 1

      append(
        Series[R](
          SeriesData(array, null),
          UniformIndex(dm.map.size),
          if newCol.isEmpty then s"${series1.name},${series2.name}" else newCol,
        )
      )

    /**
      * Appends an aggregation.
      *
      * @param aggregate
      *   Aggregation as Series.
      * @return
      *   Groups object with aggregation added.
      * @since 0.1.0
      */
    private[pd] def append[T](aggregate: Series[T]): Groups[K] =
      if dm.groupCols.contains(aggregate.name) then
        throw IllegalOperation(
          s"The group column '${aggregate.name}' cannot be used as a result column. Group columns: ${dm.groupCols.mkString(", ")}."
        )
      Groups(dm, aggregates :+ aggregate.asAny)

    /**
      * Expands the aggregation onto the original index.
      *
      * @param series
      *   Series with aggregated value per group.
      * @return
      *   Expanded series with the index of the DataMap.
      * @since 0.1.0
      */
    private[pd] def expand[T: ClassTag](series: Series[T]): Series[T] =
      var i = 0
      val it = dm.map.values.iterator
      val resultSize = dm.index.base.length
      val array = series.data.createArray(resultSize)
      while it.hasNext do
        val idx = it.next
        val v = series.data.vector(i)
        val length = idx.length
        var j = 0
        while j < length do
          array(idx(j)) = v
          j = j + 1
        i = i + 1

      Series(series.data.create(array), dm.index, series.name)

    /**
      * Array with first indices for each group.
      *
      * @return
      *   Array with indices.
      * @since 0.1.0
      */
    private[pd] def firstIndices: Array[Int] =
      var i = 0
      val it = dm.map.values.iterator
      val indices = new Array[Int](dm.map.size)
      while it.hasNext do
        indices(i) = it.next.head
        i += 1
      indices

  /**
    * @see
    *   Grouping [[https://pan-data.org/scala/operations/grouping.html]]
    * @since 0.1.0
    */
  object Groups:
    implicit def dataMapGroupsToDataFrame[K](ag: Groups[K]): DataFrame = ag.collect
