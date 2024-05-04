/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot

import pd.DataFrame
import pd.exception.ColumnNotFoundException
import pd.io.csv.CsvReader
import pd.plot.Struct.empty
import pd.plot.internal.{Config, Grammar}

import java.io.{BufferedWriter, FileWriter, Writer}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.dynamics
import scala.util.{Failure, Success, Using}

/**
  * Plotting engine based on Vega-Lite ([[https://vega.github.io/vega-lite/]]). Required DataFrame columns are
  * automatically encoded and embedded.
  *
  * The syntax keeps 1:1 to the Vega-Lite JSON grammar [[https://vega.github.io/vega-lite/docs/]] with a few adaptions:
  *   - Top level specifications are added via methods (e.g. `.title(...)`).
  *   - The mark definition is on top level (e.g. `.bar(...)`) rather than part of a `mark` definition.
  *   - The encoding channel definitions are on top level (e.g. `.color(...)`) rather than part of `encoding`.
  *
  * Top level definitions usually contain named parameters with following data types:
  *   - Number (`Int`, `Float`, `Double`),
  *   - `String`,
  *   - `Struct`, a complex struct-like object with one or more properties (of any type listed here),
  *   - `Seq[T]`, a sequence (list) where the inner type `T` is one of the types above,
  *   - `null`.
  *
  * Due to the reserved keyword `type` in Scala the following parameter name replacements were made:
  *   - Please use `as` where the JSON element is named `type` (e.g. in encodings and projection).
  *   - Please use `set` where the JSON element is named `as` (e.g. in transform).
  *
  * @see
  *   - [[https://pan-data.org/scala/basics/plotting-a-dataframe.html]] for a basic introduction
  *   - [[https://pan-data.org/scala/plotting/index.html]] for more details
  * @since 0.1.0
  */
class Plot(cfg: Config) extends Grammar[Plot](cfg):

  /**
    * How the visualization size should be determined. If a string, should be one of "pad", "fit" or "none".
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#top-level]]
    *
    * @param struct
    *   String or Struct. [[https://vega.github.io/vega-lite/docs/size.html#autosize]]
    * @since 0.1.0
    */
  def autosize(struct: Struct): Plot = addObj("autosize", struct)

  /**
    * Sets configuration object.
    *
    * [[https://vega.github.io/vega-lite/docs/config.html]]
    * @since 0.1.0
    */
  def config: Plot = setCurrent("config")

  /**
    * Color property to use as the background of the entire view.
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#top-level]]
    *
    * @param struct
    *   Color definition.
    * @since 0.1.0
    */
  def background(struct: Struct): Plot = addObj("background", struct)

  /**
    * The default visualization padding (in pixels).
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#top-level]]
    *
    * @param struct
    *   Number or Struct.
    * @example
    * {{{
    * .padding(Struct(left= 5, top=5, right= 5, bottom=5))
    * }}}
    * @since 0.1.0
    */
  def padding(struct: Struct): Plot = addObj("padding", struct)

  /**
    * Sets optional metadata.
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#top-level]]
    * @since 0.1.0
    */
  def usermeta: Plot = setCurrent("usermeta")

  // *** OTHER METHODS ***

  /**
    * Renders the plot as HTML.
    *
    * @return
    *   HTML as a string with the embedded plot.
    * @since 0.1.0
    */
  def html: String =
    s"""
       |<!DOCTYPE html>
       |<html>
       |  <head>
       |    <script src="https://cdn.jsdelivr.net/npm/vega@5.22.1"></script>
       |    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5.5.0"></script>
       |    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.21.0"></script>
       |  </head>
       |  <body>
       |    <div id="vis"></div>
       |    <script type="text/javascript">
       |      var visSpec = $json;
       |      vegaEmbed('#vis', visSpec);
       |    </script>
       |  </body>
       |</html>
       |""".stripMargin

  /**
    * Plot specification in Vega-Lite's JSON grammar.
    *
    * @return
    *   JSON as string.
    * @since 0.1.0
    */
  def json: String =
    val data = cfg.data
    Struct
      .create(
        ("$schema" -> Struct.structFromString("https://vega.github.io/schema/vega-lite/v5.json"))
          +: assemble(data)*
      )
      .toString
      .replaceAll("\"as\":", "\"type\":")
      .replaceAll("\"set\":", "\"as\":")

  /**
    * Writes plot to a HTML file.
    *
    * @param path
    *   File path.
    * @since 0.1.0
    */
  def write(path: String): Unit =
    Using(BufferedWriter(FileWriter(path)))(writer => write(writer)) match
      case Success(_) =>
      case Failure(e) => throw e

  /**
    * Writes the plots as HTML.
    *
    * @param writer
    *   Writer object.
    * @since 0.1.0
    */
  def write(writer: Writer): Unit =
    writer.write(html)

  protected def create(config: Config): Plot = new Plot(config)

/**
  * Plotting engine based on Vega-Lite.
  * @since 0.1.0
  */
object Plot:
  /**
    * Creates new [[Plot]].
    *
    * @return
    *   [[Plot]]
    *
    * @see
    *   [[https://pan-data.org/scala/basics/plotting-a-dataframe.html]] for a basic introduction
    * @see
    *   [[https://pan-data.org/scala/plotting/index.html]] for more details
    * @since 0.1.0
    */
  def apply(): Plot = new Plot(Config())

  /**
    * Creates new [[Plot]] based on a DataFrame.
    *
    * @param df
    *   DataFrame.
    * @return
    *   [[Plot]]
    *
    * @see
    *   [[https://pan-data.org/scala/basics/plotting-a-dataframe.html]] for a basic introduction
    * @see
    *   [[https://pan-data.org/scala/plotting/index.html]] for more details
    * @since 0.1.0
    */
  def apply(df: DataFrame): Plot = new Plot(Config(df))
