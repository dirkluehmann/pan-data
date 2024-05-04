/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.plot

import scala.annotation.unused
import scala.language.{dynamics, implicitConversions}

/**
  * Dynamical data structure represented in JSON format.
  *
  * @param json
  *   String JSON representation.
  *
  * @example
  *   {{{val struct = Struct(title = "apple", quantity = 3, edible=true, color=["green", "red", "green"])}}}
  * @see
  *   [[https://pan-data.org/scala/plotting/index.html]]
  * @since 0.1.0
  */
class Struct private (val json: String):

  /**
    * True if struct is empty.
    *
    * @return
    *   True if empty.
    * @since 0.1.0
    */
  def isEmptyStruct: Boolean = json.isEmpty || json == "{}"

  /**
    * True if not empty.
    *
    * @return
    *   True if not [[isEmptyStruct]].
    * @since 0.1.0
    */
  def nonEmptyStruct: Boolean = !isEmptyStruct

  /**
    * JSON string representation.
    *
    * @return
    *   JSON string.
    * @since 0.1.0
    */
  override def toString: String = json

/**
  * Dynamical data structure represented in JSON format.
  * @since 0.1.0
  */
object Struct extends Dynamic:

  /** @since 0.1.0 */
  implicit def structFromBoolean(boolean: Boolean): Struct = new Struct(boolean.toString)

  /** @since 0.1.0 */
  implicit def structFromBooleans(seq: Seq[Boolean]): Struct = createSeq(seq.map(_.toString)*)

  /** @since 0.1.0 */
  implicit def structFromDouble(double: Double): Struct = new Struct(double.toString)

  /** @since 0.1.0 */
  implicit def structFromDoubles(seq: Seq[Double]): Struct = createSeq(seq.map(_.toString)*)

  /** @since 0.1.0 */
  implicit def structFromFloat(float: Float): Struct = new Struct(float.toString)

  /** @since 0.1.0 */
  implicit def structFromFloats(seq: Seq[Float]): Struct = createSeq(seq.map(_.toString)*)

  /** @since 0.1.0 */
  implicit def structFromInt(int: Int): Struct = new Struct(int.toString)

  /** @since 0.1.0 */
  implicit def structFromInts(seq: Seq[Int]): Struct = createSeq(seq.map(_.toString)*)

  /** @since 0.1.0 */
  implicit def structFromString(string: String): Struct = new Struct("\"" + string + "\"")

  /** @since 0.1.0 */
  implicit def structFromStrings(seq: Seq[String]): Struct = createSeq(seq.map(s => s"\"$s\"")*)

  /** @since 0.1.0 */
  implicit def structFromStructs(seq: Seq[Struct]): Struct = createSeq(seq.map(_.toString)*)

  /**
    * Supports dynamic `apply` method.
    *
    * @return
    *   [[Struct]].
    * @example
    *   {{{val struct = Struct(title = "apple", quantity = 3, edible=true, color=["green", "red", "green"])}}}
    * @since 0.1.0
    */
  @unused
  def applyDynamicNamed(method: String)(kwargs: (String, Struct)*): Struct =
    if method == "apply" then create(kwargs*)
    else throw new UnsupportedOperationException(s"Method `$method` does not exist.")

  /**
    * Creates a Struct object from a JSON string.
    *
    * @param json
    *   JSON string.
    * @return
    *   [[Struct]].s
    * @since 0.1.0
    */
  def create(json: String): Struct = new Struct(json)

  /**
    * Create an empty Struct represented by "{}".
    *
    * @return
    *   Empty [[Struct]].
    * @since 0.1.0
    */
  def empty: Struct = new Struct("{}")

  /**
    * Creates a Struct represented by "null".
    *
    * @return
    *   [[Struct]] "null".
    * @since 0.1.0
    */
  def nullValue: Struct = create("null")

  /**
    * Creates a struct from keyword arguments.
    *
    * @param kwargs
    *   Tuple with keys and value.
    * @return
    *   Struct.
    * @since 0.1.0
    */
  private[pd] def create(kwargs: (String, Struct)*): Struct =
    new Struct("{" + kwargs.map(kwarg => s"\"${kwarg._1}\": ${kwarg._2}").mkString(", ") + "}\n")

  /**
    * Creates a Struct from a sequence of strings.
    *
    * @param args
    *   String values.
    * @return
    *   Struct with JSON list.
    * @since 0.1.0
    */
  private[pd] def createSeq(args: String*): Struct = new Struct(args.mkString("[", ", ", "]\n"))
