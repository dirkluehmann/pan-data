/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot

import pd.{BaseTest, DataFrame}

import java.net.URL

class PlotTest extends BaseTest:

  test("Simple plot") {
    val df = DataFrame.io.csv.read(URL("https://vega.github.io/vega-datasets/data/stocks.csv"))

    df.plot.line
      .x("date", "temporal")
      .y("price", "quantitative")
      .color("symbol", "nominal")
      .height(300)
      .width(400)
      .write("stocks1.html")

    df.plot
      .area(line = true)
      .width(400)
      .height(50)
      .x("date", Temporal, title = "Time")
      .y("price", Quantitative, title = "Price", axis = Struct(grid = false))
      .color("symbol", Nominal, legend = null)
      .row("symbol", Nominal, title = null, header = Struct(labelFontWeight = "bold", labelFontSize = 12))
      .resolve(scale = Struct(y = "independent"))
      .write("stocks2.html")

  }

  test("Layered plot") {
    val df =
      DataFrame.io.csv.read(URL("https://vega.github.io/vega-datasets/data/seattle-weather.csv"))

    df.plot
      .width(600)
      .height(300)
      .layer(
        Layer()
          .point(filled = true)
          .x(
            "date",
            Temporal,
            title = "Date",
            timeUnit = "monthdate",
            axis = Struct(format = "%b", grid = false),
          )
          .y(
            "temp_max",
            Quantitative,
            title = "Maximum Temperature (°C)",
            scale = Struct(domain = Seq(-15, 40)),
            axis = Struct(grid = true),
          )
          .color(
            "weather",
            "nominal",
            title = "Weather",
            scale = Struct(
              domain = Seq("sun", "fog", "drizzle", "rain", "snow"),
              range = Seq("#e7ba52", "#a7a7a7", "#aec7e8", "#1f77b4", "#9467bd"),
            ),
          )
      )
      .layer(
        Layer()
          .bar(height = 3, color = "#705070")
          .x(
            "date",
            Temporal,
            title = "Date",
            timeUnit = "month",
          )
          .y(
            "temp_max",
            Quantitative,
            scale = Struct(domain = Seq(-15, 40)),
            axis = null,
            aggregate = "mean",
          )
      )
      .layer(
        Layer().bar
          .x("date", Temporal, timeUnit = "month")
          .y(
            "precipitation",
            Quantitative,
            title = "Average Precipitation per Day",
            axis = Struct(
              titleColor = "#1f77b4",
              labelColor = "#1f77b4",
              values = 0 to 6 by 2,
            ),
            aggregate = "mean",
            scale = Struct(domain = Seq(0, 27.5)),
          )
      )
      .resolve(scale = Struct(y = "independent"))
      .write("weather.html")
  }
