/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.utils

import pd.DataFrame

/**
  * Helper function for string representation.
  *
  * @since 0.1.0
  */
object StringUtils:

  /**
    * String representation for n element(s).
    *
    * @param n
    *   Number of elements.
    * @return
    *   String.
    * @since 0.1.0
    */
  private[pd] def asElements(n: Int): String = if (n == 1) "1 element" else s"$n elements"

  /**
    * String of fixed size.
    * @param s
    *   String.
    * @param len
    *   Size.
    * @param leftAlign
    *   If to align left.
    * @param centerAlign
    *   If to align centered (precedence).
    * @return
    *   String of length `len`.
    * @since 0.1.0
    */
  private[pd] def fixedString(
      s: String,
      len: Int,
      leftAlign: Boolean = false,
      centerAlign: Boolean = false,
  ): String =
    val str = s.filter(c => c != '\n')
    if (len < 2)
      throw new IllegalArgumentException(
        "Column width must be greater than 1."
      )
    val actualLength = str.length
    if actualLength == len then str
    else if actualLength > len then str.substring(0, len - 2) + ".."
    else if centerAlign then
      val leftSpace = ((len - actualLength) / 2 + actualLength) max 0
      str.padTo(leftSpace, ' ').reverse.padTo(len, ' ').reverse
    else if leftAlign then str.padTo(len, ' ')
    else str.reverse.padTo(len, ' ').reverse

  /**
    * Pretty representation of objects supporting rounding for floating point numbers respecting the available width.
    *
    * @param v
    *   Any object.
    * @param maxWidth
    *   Maximum width.
    * @return
    *   String representation.
    * @since 0.1.0
    */
  private[pd] def pretty(v: Any, maxWidth: Int): String =
    v match
      case a: Array[?] =>
        if a.length > 5 then "[" + a.take(5).map(_.toString).mkString(", ") + ", ..."
        else "[" + a.map(_.toString).mkString(", ") + "]"

      case d: Double =>
        val v =
          if maxWidth > 4 && (d > 1e-4 || d < -1e-4) then
            BigDecimal(d).setScale(maxWidth - 4, BigDecimal.RoundingMode.HALF_UP).toDouble
          else d
        val s = v.toString
        if s.length > maxWidth && s.contains("E") then
          val ix = s.indexOf("E")
          val exponent = s.substring(ix)
          if maxWidth > exponent.length + 2 then
            BigDecimal(s.substring(0, s.length - exponent.length).toDouble)
              .setScale(maxWidth - exponent.length - 2, BigDecimal.RoundingMode.HALF_UP)
              .toDouble
              .toString
              + exponent
          else s
        else s

      case f: Float =>
        pretty(f.toDouble, maxWidth)
      case _ => v.toString

  /**
    * Row separator line.
    *
    * @param width
    *   Width of each column.
    * @param numColumns
    *   Number of columns.
    * @param character
    *   Character to be used.
    * @param firstWidth
    *   Width of the first column.
    * @return
    *   Row seperator string.
    * @since 0.1.0
    */
  private[pd] def separatorLine(
      width: Int,
      numColumns: Int,
      character: Char = '-',
      firstWidth: Int = 0,
  ): String =
    val initialWidth = if firstWidth == 0 then width else firstWidth
    (0 until numColumns)
      .map(i => "".padTo(if i == 0 then initialWidth else width, character))
      .mkString("+ ", " + ", " +")

  /**
    * Renders the DataFrame as a table.
    *
    * @param df
    *   DataFra.e
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
  private[pd] def table(
      df: DataFrame,
      n: Int,
      width: Int,
      annotateIndex: Boolean,
      annotateType: Boolean,
      colWidth: Int,
      indexWidth: Int,
      indexPrintName: String,
      indexPrintRepresentation: (Int, Int) => String,
  ): String =
    val s = StringBuilder()
    val m =
      if width == 0 then df.numCols
      else
        (width - (if annotateIndex then colWidth + 8 else 5)) / (colWidth + 3) match
          case x if x <= df.numCols => x
          case _                    => df.numCols
    val break = if m < df.numCols then " ...\n" else "\n"
    val line = separatorLine(
      colWidth,
      if annotateIndex then m + 1 else m,
      '-',
      if annotateIndex then indexWidth else colWidth,
    ) +
      (if m < df.numCols then " ---\n" else "\n")
    val indexSep = if m > 0 then " | " else ""
    s.append(line)
    s.append(
      df.colIndex
        .take(m)
        .map(c => fixedString(c._1, colWidth, centerAlign = true))
        .mkString(
          if annotateIndex then s"| ${fixedString(indexPrintName, indexWidth, centerAlign = true)}$indexSep" else "| ",
          " | ",
          " |" + break,
        )
    )
    if annotateType then
      s.append(
        df.colIndex
          .take(m)
          .map(c => fixedString(c._2.typeDescription(), colWidth, centerAlign = true))
          .mkString(if annotateIndex then s"| ${fixedString("", indexWidth)}$indexSep" else "| ", " | ", " |" + break)
      )
    s.append(line)
    s.append(
      df.index.iterator
        .take(n)
        .map(rowIx =>
          df.colIndex
            .take(m)
            .map(c =>
              fixedString(
                pretty(c._2(rowIx).getOrElse(fixedString("null", colWidth, centerAlign = true)), colWidth),
                colWidth,
              )
            )
            .mkString(
              if annotateIndex then s"| ${indexPrintRepresentation(rowIx, indexWidth)}$indexSep" else "| ",
              " | ",
              " |" + break,
            )
        )
        .mkString("")
    )
    s.append(line)
    if n < df.index.length then s.append(s"  showing $n of ${df.index.length} rows\n")
    if m < df.numCols then s.append(s"  showing $m of ${df.numCols} columns\n")
    s.toString
