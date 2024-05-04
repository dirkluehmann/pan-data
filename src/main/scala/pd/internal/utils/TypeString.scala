/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.utils

/**
  * Functions to map type strings for Java and Scala.
  *
  * @since 0.1.0
  */
object TypeString {

  /**
    * Maps type string representation from Java to Scala.
    *
    * @param s
    *   String with Java type representation.
    * @return
    *   Type name.
    * @since 0.1.0
    */
  def mapType(s: String): String =
    if s.endsWith("[]") then s"Array[${mapType(s.substring(0, s.length - 2)).replace("[]", "[?]")}]"
    else
      s match
        case "boolean" => "Boolean"
        case "byte"    => "Byte"
        case "char"    => "Char"
        case "double"  => "Double"
        case "float"   => "Float"
        case "int"     => "Int"
        case "long"    => "Long"
        case "short"   => "Short"
        case "Object"  => "Any"
        case s         => s

  /**
    * String representation of the vector type with (angular) brackets and (if nullable) question mark.
    *
    * @param vectorClass
    *   Class object of SeriesData vector.
    * @param containsNull
    *   Whether the vector contains undefined values.
    * @param javaType
    *   If true, returns the type following Java conventions, otherwise Scala types.
    * @return
    *   Name of class or Scala-mapped primitive name.
    * @since 0.1.0
    */
  def typeDescription(vectorClass: Class[?], containsNull: Boolean, javaType: Boolean): String =
    val t = typeString(vectorClass, javaType)
    if containsNull then if javaType then s"<$t?>" else s"[$t?]"
    else if javaType then s"<$t>"
    else s"[$t]"

  /**
    * String representation of the vector type.
    *
    * @param vectorClass
    *   Class object of SeriesData vector.
    * @param javaType
    *   If true, returns the type following Java conventions, otherwise Scala types.
    * @return
    *   Name of class or Scala-mapped primitive name.
    * @since 0.1.0
    */
  def typeString(vectorClass: Class[?], javaType: Boolean): String =
    val t = vectorClass.getSimpleName
    if javaType then if t.endsWith("[]") then t.substring(0, t.length - 2) else t
    else mapType(if t.endsWith("[]") then t.substring(0, t.length - 2) else t).replace("[]", "[?]")

  /**
    * Scala type string representation from Java reflection.
    *
    * @param value
    *   Value to inspect.
    * @return
    *   Type name.
    * @since 0.1.0
    */
  def typeString(value: Any): String = if value == null then "Null" else mapType(value.getClass.getSimpleName)

}
