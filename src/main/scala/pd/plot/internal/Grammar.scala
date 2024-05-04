/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot.internal

import pd.exception.ColumnNotFoundException
import pd.plot.*
import pd.plot.Struct.{empty, structFromString}
import pd.{DataFrame, Series}

import scala.collection.mutable
import scala.language.dynamics

/**
  * Base class for plotting specifications.
  *
  * @param cfg
  *   Plotting configuration.
  * @tparam T
  *   Type that implementes the base class.
  * @since 0.1.0
  */
abstract class Grammar[T](private[pd] val cfg: Config) extends Dynamic:

  def applyDynamicNamed(method: String)(kwargs: (String, Struct)*): T =
    if method == "apply" then
      if Grammar.seqOfMarks.contains(cfg.current) then setMark(prepend(kwargs, cfg.current))
      else if Grammar.dynamicMethods.contains(cfg.current) then addObj(cfg.current, kwargs)
      else throw UnsupportedOperationException(s"Method `${cfg.current}` does not exist.")
    else throw new UnsupportedOperationException(s"Method `$method` does not exist.")

  // *** VIEW COMPOSITION ***

  /**
    * Concatenation of plots.
    *
    * [[https://vega.github.io/vega-lite/docs/concat.html]]
    *
    * @param layers
    *   Layers.
    *
    * @see
    *   [[hconcat]], [[vconcat]]
    * @since 0.1.0
    */
  def concat(columns: Int, layers: Layer*): T =
    create(addLayers(layers).setLayerMode("concat").addObj("columns", columns))

  /**
    * Horizontal concatenation of plots.
    *
    * [[https://vega.github.io/vega-lite/docs/concat.html]]
    *
    * @param layers
    *   Layers.
    * @since 0.1.0
    */
  def hconcat(layers: Layer*): T =
    create(addLayers(layers).setLayerMode("hconcat"))

  /**
    * Adds multiple (overlaying) layers .
    *
    * [[https://vega.github.io/vega-lite/docs/layer.html]]
    *
    * @param layers
    *   Layers.
    *
    * @note
    *   The layering mode is applied to all defined layers.
    * @since 0.1.0
    */
  def layer(layers: Layer*): T =
    create(addLayers(layers).setLayerMode("layer"))

  /**
    * Vertical concatenation of plots.
    *
    * [[https://vega.github.io/vega-lite/docs/concat.html]]
    *
    * @param layers
    *   Layers.
    * @since 0.1.0
    */
  def vconcat(layers: Layer*): T =
    create(addLayers(layers).setLayerMode("vconcat"))

  // *** VIEW SPECIFICATION***

  /**
    * The alignment to apply to grid rows and columns.
    *
    * @param struct
    *   [[https://vega.github.io/vega-lite/docs/spec.html#common]]
    * @since 0.1.0
    */
  def align(struct: Struct*): T = addObj("align", struct)

  /**
    * The bounds calculation method to use for determining the extent of a sub-plot.
    *
    * @param struct
    *   [[https://vega.github.io/vega-lite/docs/spec.html#common]]
    * @since 0.1.0
    */
  def bounds(struct: Struct*): T = addObj("bounds", struct)

  /**
    * Indicating if subviews should be centered relative to their respective rows or columns.
    *
    * @param struct
    *   [[https://vega.github.io/vega-lite/docs/spec.html#common]]
    * @since 0.1.0
    */
  def center(struct: Struct*): T = addObj("center", struct)

  /**
    * Custom element.
    *
    * @param name
    *   Element name.
    * @param struct
    *   Definition.
    * @since 0.1.0
    */
  def custom(name: String, struct: Struct): T = addObj(name, struct)

  /**
    * Custom encoding channel element.
    *
    * @param name
    *   Encoding name.
    * @param field
    *   Column or field name.
    * @param struct
    *   Definition.
    * @since 0.1.0
    */
  def customEncoding(name: String, field: String, struct: Struct): T = addEncoding(name, field, struct)

  /**
    * Custom data definition.
    *
    * [[https://vega.github.io/vega-lite/docs/data.html]]
    * @since 0.1.0
    */
  def data: T = setCurrent("data")

  /**
    * DataFrame referenced by column names for Plot or Layer.
    *
    * @param df
    *   DataFrame.
    * @since 0.1.0
    */
  def dataFrame(df: DataFrame): T = create(cfg.setDf(df))

  /**
    * Embeds additional DataFrame columns in the data.
    *
    * @param field
    *   Column name.
    * @since 0.1.0
    */
  def dataInclude(field: String*): T = create(cfg.addData(field))

  /**
    * Description of this mark for commenting purpose.
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#common]]
    *
    * @param string
    *   Description.
    * @since 0.1.0
    */
  def description(string: String): T = addObj("description", string)

  /**
    * Height of plot.
    *
    * [[https://vega.github.io/vega-lite/docs/size.html]]
    *
    * @param px
    *   Height of plot.
    * @since 0.1.0
    */
  def height(px: Int): T = addObj("height", px)

  /**
    * Name of the visualization for reference.
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#common]]
    *
    * @param name
    *   Name.
    * @since 0.1.0
    */
  def name(name: String): T = addObj("name", name)

  /**
    * Parameters can either be simple variables or more complex selections that map user input to data queries.
    *
    * @param struct
    *   [[https://vega.github.io/vega-lite/docs/parameter.html]]
    * @since 0.1.0
    */
  def param(struct: Struct*): T = addObj("param", struct)

  /**
    * Sets cartographic projection.
    *
    * [[https://vega.github.io/vega-lite/docs/projection.html]]
    * @since 0.1.0
    */
  def projection: T = setCurrent("projection")

  /**
    * Scale, axis, and legend resolutions for view composition specifications.
    *
    * [[https://vega.github.io/vega-lite/docs/resolve.html]]
    *
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/resolve.html]]
    * @param axis
    *   [[https://vega.github.io/vega-lite/docs/resolve.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/resolve.html]]
    * @since 0.1.0
    */
  def resolve(scale: Struct = empty, axis: Struct = empty, legend: Struct = empty): T =
    addObj("resolve", Seq[(String, Struct)]("scale" -> scale, "axis" -> axis, "legend" -> legend))

  /**
    * The spacing (in pixels) between sub-views of the composition operator.
    *
    * @param struct
    *   [[https://vega.github.io/vega-lite/docs/spec.html#common]]
    * @since 0.1.0
    */
  def spacing(struct: Struct*): T = addObj("spacing", struct)

  /**
    * Plot title.
    *
    * [[https://vega.github.io/vega-lite/docs/title.html]]
    *
    * @param title
    *   [[https://vega.github.io/vega-lite/docs/title.html]]
    * @since 0.1.0
    */
  def title(title: Struct): T = addObj("title", title)

  /**
    * Data transformations such as filter and new field calculation.
    *
    * @param struct
    *   [[https://vega.github.io/vega-lite/docs/transform.html]]
    *
    * @note
    *   Use "set" to set a new field/variable.
    *
    * @example
    * {{{
    * .transform(
    *   Struct(calculate="datum.x*datum.x", set="x2"),
    *   Struct(filter="datum.x2 < 100")
    *   )
    * }}}
    * @since 0.1.0
    */
  def transform(struct: Struct*): T = addObj("transform", struct)

  /**
    * Defines the view background’s fill and stroke.
    *
    * [[https://vega.github.io/vega-lite/docs/spec.html#view-background]]
    * @since 0.1.0
    */
  def view: T = setCurrent("view")

  /**
    * Width of plot.
    *
    * [[https://vega.github.io/vega-lite/docs/size.html]]
    *
    * @param px
    *   Width of plot.
    * @since 0.1.0
    */
  def width(px: Int): T = addObj("width", px)

  // *** MARKS ***
  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/arc.html]]
    * @since 0.1.0
    */
  def arc: T = setMark("arc")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/area.html]]
    * @since 0.1.0
    */
  def area: T = setMark("area")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/bar.html]]
    * @since 0.1.0
    */
  def bar: T = setMark("bar")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/boxplot.html]]
    * @since 0.1.0
    */
  def boxplot: T = setMark("boxplot")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/circle.html]]
    * @since 0.1.0
    */
  def circle: T = setMark("circle")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/errorband.html]]
    * @since 0.1.0
    */
  def errorband: T = setMark("errorband")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/errorbar.html]]
    * @since 0.1.0
    */
  def errorbar: T = setMark("errorbar")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/geoshape.html]]
    * @since 0.1.0
    */

  def geoshape: T = setMark("geoshape")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/line.html]]
    * @since 0.1.0
    */
  def line: T = setMark("line")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/point.html]]
    * @since 0.1.0
    */
  def point: T = setMark("point")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/rect.html]]
    * @since 0.1.0
    */
  def rect: T = setMark("rect")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/rule.html]]
    * @since 0.1.0
    */
  def rule: T = setMark("rule")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/square.html]]
    * @since 0.1.0
    */
  def square: T = setMark("square")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/text.html]]
    * @since 0.1.0
    */
  def text: T = setMark("text")

  /**
    * @see
    *   [[https://vega.github.io/vega-lite/docs/tick.html]]
    * @since 0.1.0
    */
  def tick: T = setMark("tick")

  // *** POSITION CHANNELS ***

  /**
    * X coordinates or width of horizontal "bar" and "area".
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#position]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param axis
    *   [[https://vega.github.io/vega-lite/docs/axis.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param impute
    *   [[https://vega.github.io/vega-lite/docs/impute.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param stack
    *   [[https://vega.github.io/vega-lite/docs/stack.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def x(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      axis: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      impute: Struct = empty,
      scale: Struct = empty,
      sort: Struct = empty,
      stack: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "x",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "axis" -> axis,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "impute" -> impute,
        "scale" -> scale,
        "sort" -> sort,
        "stack" -> stack,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * The xOffset determines an additional offset to the x position.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#positon-offset]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def xOffset(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      scale: Struct = empty,
      sort: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "xOffset",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "scale" -> scale,
        "sort" -> sort,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * X2 coordinates for ranged "area", "bar", "rect", and "rule".
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#position]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param impute
    *   [[https://vega.github.io/vega-lite/docs/impute.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def x2(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      bandPosition: Struct = empty,
      datum: Struct = empty,
      impute: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "x2",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "impute" -> impute,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Y coordinates or height of vertical "bar" and "area".
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#position]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param axis
    *   [[https://vega.github.io/vega-lite/docs/axis.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param impute
    *   [[https://vega.github.io/vega-lite/docs/impute.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param stack
    *   [[https://vega.github.io/vega-lite/docs/stack.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def y(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      axis: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      impute: Struct = empty,
      scale: Struct = empty,
      sort: Struct = empty,
      stack: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "y",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "axis" -> axis,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "impute" -> impute,
        "scale" -> scale,
        "sort" -> sort,
        "stack" -> stack,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * The yOffset determines an additional offset to the y position.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#positon-offset]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def yOffset(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      scale: Struct = empty,
      sort: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "yOffset",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "scale" -> scale,
        "sort" -> sort,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Y2 coordinates for ranged "area", "bar", "rect", and "rule".
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#position]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param impute
    *   [[https://vega.github.io/vega-lite/docs/impute.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def y2(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      bandPosition: Struct = empty,
      datum: Struct = empty,
      impute: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "y2",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "impute" -> impute,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )
  // *** POLAR POSITION CHANNELS ***

  /**
    * Theta determines the position or interval on polar coordinates for arc and text marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#polar]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param stack
    *   [[https://vega.github.io/vega-lite/docs/stack.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def theta(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      scale: Struct = empty,
      sort: Struct = empty,
      stack: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "theta",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "scale" -> scale,
        "sort" -> sort,
        "stack" -> stack,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Theta2 determines the interval on polar coordinates for arc and text marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#polar]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def theta2(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "theta2",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Radius determines the position or interval on polar coordinates for arc and text marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#polar]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]] *
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param stack
    *   [[https://vega.github.io/vega-lite/docs/stack.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def radius(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      scale: Struct = empty,
      sort: Struct = empty,
      stack: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "radius",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "scale" -> scale,
        "sort" -> sort,
        "stack" -> stack,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Radius2 determines the position or interval on polar coordinates for arc and text marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#polar]]
    *
    * * @param field Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def radius2(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "radius2",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  // *** GEOGRAPHIC POSITION CHANNELS ***

  /**
    * Longitude position of geographically projected marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#geo]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def longitude(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "longitude",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Longitude2 position for geographically projected ranged "area", "bar", "rect", and "rule".
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#geo]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    *
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def longitude2(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "longitude2",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Latitude position of geographically projected marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#geo]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def latitude(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "latitude",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Latitude2 position for geographically projected ranged "area", "bar", "rect", and "rule"
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#geo]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def latitude2(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "latitude2",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  // *** MARK PROPERTY CHANNELS ***

  /**
    * Rotation angle of "point" and "text" marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def angle(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "angle",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Color of the marks – either fill or stroke color based on the filled property of mark definition.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def color(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "color",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Fill color of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def fill(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "fill",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Stroke color of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def stroke(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "stroke",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Opacity of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def opacity(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "opacity",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Fill opacity of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def fillOpacity(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "fillOpacity",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Stroke opacity of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def strokeOpacity(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "strokeOpacity",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Shape of the mark.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def shape(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "shape",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Size of the mark.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def size(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "size",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Stroke dash of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def strokeDash(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "strokeDash",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Stroke width of the marks.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#mark-prop]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param legend
    *   [[https://vega.github.io/vega-lite/docs/legend.html]]
    * @param scale
    *   [[https://vega.github.io/vega-lite/docs/scale.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def strokeWidth(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      legend: Struct = empty,
      scale: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "strokeWidth",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "legend" -> legend,
        "scale" -> scale,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  // *** TEXT AND TOOLTIP CHANNELS ***

  /**
    * Text of the text mark.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#text]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param format
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param formatType
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def text(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      format: Struct = empty,
      formatType: String = "",
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "text",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "format" -> format,
        "formatType" -> formatType,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * The tooltip text to show upon mouse hover.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#text]]
    *
    * [[https://vega.github.io/vega-lite/docs/tooltip.html]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param format
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param formatType
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def tooltip(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      format: Struct = empty,
      formatType: String = "",
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "tooltip",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "format" -> format,
        "formatType" -> (if formatType.isEmpty then empty else formatType),
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Multi-field tooltip text to show upon mouse hover.
    *
    * [[https://vega.github.io/vega-lite/docs/tooltip.html]]
    *
    * @param fields
    *   Tuples of field names and Struct(field, as, aggregate, bandPosition, bin, condition, datum, format, formatType,
    *   timeUnit, title, value) (see [[tooltip]] for details).
    *
    * @example
    * {{{
    * .tooltips(
    *   "x" -> Struct(field = "x", as = Temporal, title = "X Value"),
    *   "y" -> Struct(field = "y", as = Nominal, title = "Y Value")
    * )
    * }}}
    * @since 0.1.0
    */
  def tooltips(fields: (String, Struct)*): T =
    addEncoding("tooltip", fields.map(_._1), fields.map(_._2))

  // *** HYPERLINK CHANNEL ***

  /**
    * The href encoding makes a mark a hyperlink.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#href]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def href(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "href",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  // ** LEVEL OF DETAIL CHANNEL ***

  /**
    * Defines an additional grouping field for grouping data without mapping the field to any visual properties.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#detail]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @since 0.1.0
    */
  def detail(field: String = "", as: String = ""): T =
    addEncoding(
      "detail",
      field,
      Seq[(String, Struct)]("field" -> field, "type" -> as),
    )

  /**
    * Defines additional grouping fields for grouping data without mapping the fields to any visual properties.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#detail]]
    *
    * @param fields
    *   Tuples of `field` and `as` ([[https://vega.github.io/vega-lite/docs/type.html]]).
    *
    * Example:
    * {{{
    * .details( "group3" -> Nominal, "group4" -> Nominal)
    * }}}
    * @since 0.1.0
    */
  def details(fields: (String, String)*): T =
    addEncoding(
      "detail",
      fields.map(_._1),
      fields.map(v => Struct(field = v._1, as = v._2)),
    )

  // *** DESCRIPTION CHANNEL ***

  /**
    * The description encoding adds a text description to the mark for ARIA accessibility (SVG output only).
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#text]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param format
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param formatType
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def description(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      format: Struct = empty,
      formatType: String = "",
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "description",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "format" -> format,
        "formatType" -> formatType,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  // *** KEY CHANNEL ***

  /**
    * The key channel can enable object constancy for transitions over dynamic data.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#key]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param format
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param formatType
    *   [[https://vega.github.io/vega-lite/docs/format.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def key(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      datum: Struct = empty,
      format: Struct = empty,
      formatType: String = "",
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "key",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "datum" -> datum,
        "format" -> format,
        "formatType" -> formatType,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  // *** ORDER CHANNEL ***

  /**
    * The order defines a data field that is used to sorts stacking order for stacked charts and the order of data
    * points in line marks for connected scatterplots.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#order]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param aggregate
    *   [[https://vega.github.io/vega-lite/docs/aggregate.html]]
    * @param bandPosition
    *   [[https://vega.github.io/vega-lite/docs/band.html]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/bin.html]]
    * @param condition
    *   [[https://vega.github.io/vega-lite/docs/condition.html]]
    * @param datum
    *   [[https://vega.github.io/vega-lite/docs/datum.html]]
    * @param sort
    *   [[https://vega.github.io/vega-lite/docs/sort.html]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/timeunit.html]]
    * @param title
    *   Title.
    * @param value
    *   [[https://vega.github.io/vega-lite/docs/value.html]]
    * @since 0.1.0
    */
  def order(
      field: String = "",
      as: String = "",
      aggregate: Struct = empty,
      bandPosition: Struct = empty,
      bin: Struct = empty,
      condition: Struct = empty,
      datum: Struct = empty,
      sort: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
      value: Struct = empty,
  ): T =
    addEncoding(
      "order",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "aggregate" -> aggregate,
        "bandPosition" -> bandPosition,
        "bin" -> bin,
        "condition" -> condition,
        "datum" -> datum,
        "sort" -> sort,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "value" -> value,
      ),
    )

  /**
    * Multi-field order definition.
    *
    * [[https://vega.github.io/vega-lite/docs/encoding.html#order]]
    *
    * @param fields
    *   Tuples of field names and Struct.
    * @since 0.1.0
    */
  def order(fields: (String, Struct)*): T =
    addEncoding("order", fields.map(_._1), fields.map(_._2))

  // *** FACET CHANNELS ***
  /**
    * Facets single plots into trellis plots, i.e. columns and rows.
    *
    * @see
    *   [[https://vega.github.io/vega-lite/docs/facet.html]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param align
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param center
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param columns
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param header
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param spacing
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param title
    *   Title.
    * @since 0.1.0
    */
  def facet(
      field: String,
      as: String = "",
      align: Struct = empty,
      bin: Struct = empty,
      center: Struct = empty,
      columns: Struct = empty,
      header: Struct = empty,
      spacing: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
  ): T =
    addEncoding(
      "facet",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "bin" -> bin,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "header" -> header,
        "columns" -> columns,
        "align" -> align,
        "center" -> center,
        "spacing" -> spacing,
      ),
    )

  /**
    * Facets plots into columns.
    *
    * @see
    *   [[https://vega.github.io/vega-lite/docs/facet.html]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param align
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param center
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param header
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param spacing
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param title
    *   Title.
    * @since 0.1.0
    */
  def column(
      field: String,
      as: String = "",
      align: Struct = empty,
      bin: Struct = empty,
      center: Struct = empty,
      header: Struct = empty,
      spacing: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
  ): T =
    addEncoding(
      "column",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "bin" -> bin,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "header" -> header,
        "align" -> align,
        "center" -> center,
        "spacing" -> spacing,
      ),
    )

  /**
    * Facets plots into rows.
    *
    * @see
    *   [[https://vega.github.io/vega-lite/docs/facet.html]]
    *
    * @param field
    *   Column or field name.
    * @param as
    *   [[https://vega.github.io/vega-lite/docs/type.html]]
    * @param align
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param bin
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param center
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param header
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param spacing
    *   [[https://vega.github.io/vega-lite/docs/facet.html#facet-field-definition]]
    * @param timeUnit
    *   [[https://vega.github.io/vega-lite/docs/facet.html#field-def]]
    * @param title
    *   Title.
    * @since 0.1.0
    */
  def row(
      field: String,
      as: String = "",
      align: Struct = empty,
      bin: Struct = empty,
      center: Struct = empty,
      header: Struct = empty,
      spacing: Struct = empty,
      timeUnit: Struct = empty,
      title: Struct = empty,
  ): T =
    addEncoding(
      "row",
      field,
      Seq[(String, Struct)](
        "field" -> field,
        "type" -> as,
        "bin" -> bin,
        "timeUnit" -> timeUnit,
        "title" -> title,
        "header" -> header,
        "align" -> align,
        "center" -> center,
        "spacing" -> spacing,
      ),
    )

  // *** ASSEMBLE METHODS ***

  /** @since 0.1.0 */
  private[pd] def assemble(data: Seq[String] = cfg.data): Seq[(String, Struct)] =
    val b = mutable.SeqMap[String, Struct]()
    b.put("mark", cfg.mark)
    b.put("encoding", Struct.create(cfg.encoding.toSeq*))
    cfg.objects.foreach(o => b.put(o._1, o._2))
    if cfg.layers.nonEmpty then b.put(cfg.layerMode, cfg.layers)
    generateData(data) match
      case Some(values) => b.put("data", Struct(values = values))
      case None         =>
    b.toSeq

  /** @since 0.1.0 */
  private[pd] def generateData(dataColumns: Seq[String]): Option[Struct] =
    if cfg.df.isEmpty || dataColumns.isEmpty then None
    else
      val df = cfg.df.get
      val data = cfg.data
      val columns = df.columns
      data.filterNot(columns.contains).foreach(c => throw ColumnNotFoundException(df, c))
      val cols: Seq[Series[String]] = data
        .map(df(_))
        .map(s =>
          val prefix = s"\"${s.name}\": "
          val json =
            (if s.isInt || s.isDouble || s.isBoolean then prefix +: s.str
             else (s"$prefix\"" +: s.str) + "\"")
          json.fill("")
        )
      val values = cols.tail.fold(cols.head)((s1, s2) =>
        s1.map(
          s2,
          (a, b) =>
            if a.nonEmpty && b.nonEmpty then s"$a, $b" else if a.nonEmpty then a else if b.nonEmpty then b else "",
        )
      )
      val result = values.iterator.map(row => s"{${row.getOrElse("")}}").mkString(", ")
      Some(Struct.create(s"[$result]\n"))

  // *** HELPER METHODS ***

  /** @since 0.1.0 */
  private[pd] def addEncoding(key: String, field: String, kwargs: Seq[(String, Struct)]): T =
    create(
      cfg.addEncoding(
        key,
        field,
        Struct.create(
          cleanNull(autoType(field, kwargs)).filterNot((a, b) =>
            b.isEmptyStruct || b.toString == "\"\"" && (a == "field" || a == "type")
          )*
        ),
      )
    )

  /** @since 0.1.0 */
  private[pd] def addEncoding(key: String, field: String, struct: Struct): T =
    create(cfg.addEncoding(key, field, struct))

  /** @since 0.1.0 */
  private[pd] def addEncoding(key: String, fields: Seq[String], struct: Struct): T =
    create(cfg.addEncoding(key, fields, struct))

  /** @since 0.1.0 */
  private[pd] def addLayers(layers: Seq[Layer]): Config =
    val data = layers.foldLeft(Seq[String]())((data, layer) =>
      if (layer.cfg.objects.contains("data") || layer.cfg.df.isDefined) data else data ++ layer.cfg.data
    )
    cfg.addData(data).addLayers(layers.map(_.toStruct))

  /** @since 0.1.0 */
  private[pd] def addObj(field: String): T =
    create(cfg.addObj(field, Struct.empty))

  /** @since 0.1.0 */
  private[pd] def addObj(key: String, obj: Struct): T =
    create(cfg.addObj(key, cleanNull(obj)))

  /** @since 0.1.0 */
  private[pd] def addObj(key: String, kwargs: Seq[(String, Struct)]): T =
    create(cfg.addObj(key, Struct.create(cleanNull(kwargs)*)))

  /** @since 0.1.0 */
  private[pd] def autoType(field: String, seq: Seq[(String, Struct)]): Seq[(String, Struct)] =
    seq.map(t =>
      if t._1 == "type" && (t._2 == null || t._2.toString == "\"\"") then
        if (cfg.df.isDefined && cfg.df.get.contains(field))
          val typeString = cfg.df.get(field).typeString
          if typeString == "Int" | typeString == "Double" | typeString == "Long" | typeString == "Float" then
            ("as", Quantitative)
          else if typeString == "LocalDate" | typeString == "LocalDateTime" | typeString == "LocalTime"
              | typeString == "ZonedDateTime" | typeString == "Date"
          then ("as", Temporal)
          else if typeString == "Boolean" then ("as", Ordinal)
          else ("as", Nominal)
        else t
      else t
    )

  /** @since 0.1.0 */
  private[pd] def prepend(kwargs: Seq[(String, Struct)], value: Struct, key: String = "type"): Struct =
    Struct.create((key -> value) +: kwargs*)

  /** @since 0.1.0 */
  private[pd] def setCurrent(key: String): T =
    create(cfg.setCurrent(key))

  /** @since 0.1.0 */
  private[pd] def setMark(key: String): T =
    create(cfg.setCurrent(key).setMark(Struct(as = key)))

  /** @since 0.1.0 */
  private[pd] def setMark(obj: Struct): T =
    create(cfg.setMark(cleanNull(obj)))

  // *** ABSTRACT METHODS ***

  /** @since 0.1.0 */
  protected def create(config: Config): T

  // *** PRIVATE METHODS ***

  /** @since 0.1.0 */
  private def cleanNull(seq: Seq[(String, Struct)]): Seq[(String, Struct)] =
    seq.map(t => if t._2 == null then (t._1, Struct.create("null")) else t)

  /** @since 0.1.0 */
  private def cleanNull(obj: Struct): Struct =
    if obj == null then Struct.create("null") else obj

/**
  * Plotting specifications constants.
  * @since 0.1.0
  */
object Grammar:

  /** @since 0.1.0 */
  private val seqOfMarks: Seq[String] = Seq(
    "area",
    "bar",
    "circle",
    "errorband",
    "errorbar",
    "geoshape",
    "line",
    "point",
    "rect",
    "rule",
    "square",
    "text",
    "tick",
    "boxplot",
  )

  /** @since 0.1.0 */
  private val dynamicMethods: Seq[String] = Seq(
    "config",
    "data",
    "projection",
    "title",
    "usermeta",
    "view",
  )
