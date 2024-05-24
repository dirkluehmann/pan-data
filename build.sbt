/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

name := "pan-data"
ThisBuild / version := "0.0.2"

organization := "org.pan-data"
homepage := Some(url("https://pan-data.org"))
licenses := Seq("Mozilla Public License 2.0" -> url("https://www.mozilla.org/en-US/MPL/2.0/"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/dirkluehmann/pan-data"),
    "scm:git@github.com:dirkluehmann/pan-data.git",
  )
)

ThisBuild / scalaVersion := "3.0.2"
Compile / compileOrder := CompileOrder.ScalaThenJava

libraryDependencies ++= Seq(
  "com.univocity" % "univocity-parsers" % "2.9.1",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
)

Test / parallelExecution := false
Test / publishArtifact := false

publishMavenStyle := true
publishTo := Some(MavenCache("local-maven", file("releases")))
Compile / packageDoc / publishArtifact := true
Compile / packageSrc / publishArtifact := true

developers := List(
  Developer(
    id="dirk@pan-data.org",
    name="D.-S. Luehmann",
    email="dirk@pan-data.org",
    url=url("https://pan-data.org")
  )
)
