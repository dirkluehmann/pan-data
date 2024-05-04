/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2023-2024, initial author: Dirk-Soeren Luehmann
 */

package pd.internal.series

import pd.Settings
import pd.exception.*
import pd.internal.index.BaseIndex
import pd.internal.series.SeriesData.fromIterator
import pd.internal.utils.StringUtils.asElements
import pd.internal.utils.{RequireType, TypeString}

import scala.annotation.unused
import scala.collection.mutable
import scala.language.postfixOps
import scala.reflect.{ClassTag, Typeable}

/**
  * A masked array for low-level operations.
  *
  * @param vector
  *   Array with data.
  * @param mask
  *   Null, if all elements in the vector are defined (`containsNull` is false), or a boolean mask, where for each false
  *   the respective element in the vector is undefined.
  * @since 0.1.0
  */
private[pd] class SeriesData[T](
    private[pd] val vector: Array[T],
    private[pd] val mask: Array[Boolean] = null,
):

  /**
    * True, if the array has undefined value (mask is not null), or otherwise false (mask is null).
    *
    * @since 0.1.0
    */
  val containsNull: Boolean = mask != null

  /**
    * Value of index position `ix` as an Option. If mask if false for `ix`, `None` is returned.
    *
    * Use vector and mask directly for higher performance.
    *
    * @param ix
    *   Index position.
    * @return
    *   Value as an Option.
    * @since 0.1.0
    */
  inline def apply(ix: Int): Option[T] = if mask == null || mask(ix) then Some(vector(ix)) else None

  /**
    * Returns the same DataSeries but with the inner type cast to `T2`.
    *
    * @return
    *   DataSeries as instance of `T2`.
    * @since 0.1.0
    */
  inline def as[T2]: SeriesData[T2] = this.asInstanceOf[SeriesData[T2]]

  /**
    * Clones the SeriesData by copying the vector and (if applicable) the mask array.
    *
    * @return
    *   Copy of the SeriesData.
    * @since 0.1.0
    */
  override def clone: SeriesData[T] =
    if mask == null then new SeriesData[T](vector.clone(), null)
    else new SeriesData[T](vector.clone(), mask.clone())

  /**
    * Clones the SeriesData by copying the vector and (if applicable) the mask array. Transforms the mask to `null` if
    * it has only true elements.
    *
    * @return
    *   Copy of the SeriesData.
    * @since 0.1.0
    */
  def cloneSafely: SeriesData[T] =
    if mask == null then new SeriesData[T](vector.clone(), null)
    else if mask.contains(false) then new SeriesData[T](vector.clone(), mask.clone())
    else new SeriesData[T](vector.clone(), null)

  /**
    * Creates a DataSeries with the same type.
    *
    * @param vector
    *   Vector.
    * @param mask
    *   Mask.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  def create(vector: Array[T], mask: Array[Boolean] = null): SeriesData[T] = new SeriesData[T](vector, mask)

  /**
    * Creates an Array of the same type as the current [[vector]] (irrespectively instance type T at the callers
    * context) using Java reflection.
    *
    * @param size
    *   Size of the array.
    * @return
    *   Array.
    * @since 0.1.0
    */
  def createArray(size: Int = vector.length): Array[T] =
    java.lang.reflect.Array.newInstance(vector.getClass.getComponentType, size).asInstanceOf[Array[T]]

  /**
    * Creates an empty SeriesData object.
    * @return
    *   SeriesData with an empty vector.
    * @since 0.1.0
    */
  def createEmpty: SeriesData[T] = new SeriesData[T](createArray(0), null)

  /**
    * Describes the SeriesData.
    *
    * @return
    *   Info string.
    * @since 0.1.0
    */
  def describe: String = s"The data type is ${typeDescription()} " +
    (if containsNull then s"with undefined entries."
     else "with no undefined entries.") +
    s"\n  The underlying data vector has ${asElements(length)}."

  /**
    * Element-wise comparison.
    *
    * @param that
    *   SeriesData to compare with.
    * @return
    *   True if equal.
    * @since 0.1.0
    */
  def equivalent(that: SeriesData[?]): Boolean = vector sameElements that.vector

  /**
    * Value of index position `ix`. If mask if false for `ix`, a random value is returned.
    *
    * Use vector and mask directly for higher performance.
    *
    * @param ix
    *   Index position.
    * @return
    *   Value.
    * @since 0.1.0
    */
  inline def get(ix: Int): T = vector(ix)

  /**
    * Mask for position `ix`.
    *
    * Use vector and mask directly for higher performance.
    *
    * @param ix
    *   Index position.
    * @return
    *   Mask with false if index position is missing.
    * @since 0.1.0
    */
  inline def getMask(ix: Int): Boolean = mask == null || mask(ix)

  /**
    * Inverse mask for position `ix`.
    *
    * Use vector and mask directly for higher performance.
    *
    * @param ix
    *   Index position.
    * @return
    *   True if index position is missing.
    * @since 0.1.0
    */
  inline def isEmpty(ix: Int): Boolean = !(mask == null || mask(ix))

  /**
    * Length of the vector.
    *
    * @return
    *   Length.
    * @since 0.1.0
    */
  inline def length: Int = vector.length

  /**
    * Copies and masks elements None that are not in `index`.
    *
    * @param index
    *   Source index.
    * @return
    *   New SeriesData.
    * @since 0.1.0
    */
  def maskIndex(index: BaseIndex): SeriesData[T] =
    SeriesData.fromIndex(this, index, length)

  /**
    * Copies with new size and masks elements None that are not in `index`.
    *
    * @param index
    *   Source index.
    * @param size
    *   New size.
    * @return
    *   New SeriesData.
    * @since 0.1.0
    */
  def maskIndexResize(index: BaseIndex, size: Int): SeriesData[T] =
    SeriesData.fromIndex(this, index, size)

  /**
    * Resizes vector. If size is not changed, the vector is not copied.
    *
    * @param size
    *   New size.
    * @return
    * @since 0.1.0
    */
  def resize(index: BaseIndex, size: Int): SeriesData[T] =
    if length == size then this else SeriesData.fromIndex(this, index, size, None)

  /**
    * A string representation of all the elements of the vector.
    * @return
    *   Concatenated elements as a String.
    * @since 0.1.0
    */
  override def toString: String = vector.mkString(":")

  /**
    * The type representation with (angular) brackets and (if nullable) question mark.
    *
    * @param javaType
    *   If true, returns the type following Java conventions, otherwise Scala types.
    * @return
    * @since 0.1.0
    */
  def typeDescription(javaType: Boolean = Settings.printJavaType): String =
    TypeString.typeDescription(vector.getClass, containsNull, javaType)

  /**
    * String representation of vector type.
    *
    * @param javaType
    *   If true, returns the type following Java conventions, otherwise Scala types.
    * @return
    *   Name of class or Scala-mapped primitive name.
    * @since 0.1.0
    */
  def typeString(javaType: Boolean = Settings.printJavaType): String =
    TypeString.typeString(vector.getClass, javaType)

  /**
    * Java class object of the inner type of the vector.
    *
    * @return
    *   Class of vector.
    * @since 0.1.0
    */
  def vectorClass: Class[?] = vector.getClass.getComponentType

/**
  * A masked array for low-level operations.
  * @since 0.1.0
  */
private[pd] object SeriesData:

  private type SupportedCollections[T] = Seq[T] | Array[T] | mutable.Buffer[T]

  /**
    * A SeriesData object with an empty vector.
    *
    * @return
    *   An empty SeriesData object.
    * @since 0.1.0
    */
  def empty[T: ClassTag]: SeriesData[T] = new SeriesData[T](new Array[T](0), null)

  /**
    * A SeriesData object with a vector of length `length` but with mask false for all elements.
    *
    * @return
    *   An empty SeriesData object.
    * @since 0.1.0
    */

  def empty[T: ClassTag](length: Int): SeriesData[T] =
    new SeriesData[T](new Array[T](length), Array.fill(length)(false))

  /**
    * The elements of the array are copied into the SeriesData object.
    *
    * @param data
    *   Collection.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  def fromArray[T](data: Array[T]): SeriesData[T] =
    var containsNull = false
    var ix = 0

    val array = data.clone()
    val size = array.length
    val mask = Array.fill[Boolean](size)(true)

    while ix < size do
      val v = array(ix)
      if v == null then
        containsNull = true
        mask(ix) = false
      else array(ix) = v
      ix = ix + 1

    SeriesData(array, if containsNull then mask else null)

  /**
    * The elements of the collections are copied into the SeriesData object.
    *
    * @param data
    *   Collection.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  def fromCollection[T: ClassTag](data: SupportedCollections[T]): SeriesData[T] =
    import scala.language.implicitConversions
    implicit def convert(it: Iterator[?]): Iterator[T] = it.asInstanceOf[Iterator[T]]
    data match
      case data: Array[?]          => fromIterator(data.iterator, data.size)
      case data: Seq[?]            => fromIterator(data.iterator, data.size)
      case data: mutable.Buffer[?] => fromIterator(data.iterator, data.size)

  /**
    * The elements of the collections are copied into the SeriesData object.
    *
    * @param data
    *   Collection.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  def fromCollectionWithNull[T: ClassTag](data: SupportedCollections[T | Null]): SeriesData[T] =
    import scala.language.implicitConversions
    // noinspection ScalaRedundantCast
    @unused
    implicit def convert(it: Iterator[?]): Iterator[T | Null] = it.asInstanceOf[Iterator[T | Null]]

    data match
      case data: Array[?]          => fromIteratorOrNull(data.iterator, data.size)
      case data: Seq[?]            => fromIteratorOrNull(data.iterator, data.size)
      case data: mutable.Buffer[?] => fromIteratorOrNull(data.iterator, data.size)

  /**
    * The Option elements of the collections and copied into the SeriesData object if defined.
    *
    * @param data
    *   Collection of Option.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  def fromOptionCollection[T: ClassTag](data: SupportedCollections[Option[T]]): SeriesData[T] =
    import scala.language.implicitConversions
    implicit def convert(it: Iterator[?]): Iterator[Option[T]] = it.asInstanceOf[Iterator[Option[T]]]

    data match
      case data: Array[?]          => fromIteratorOption(data.iterator, data.size)
      case data: Seq[?]            => fromIteratorOption(data.iterator, data.size)
      case data: mutable.Buffer[?] => fromIteratorOption(data.iterator, data.size)

  /**
    * Data is checked and copied into an array.
    *
    * @param iterator
    *   Iterator corresponding to an uniform index.
    * @param size
    *   Number of elements of the `iterator` to process.
    * @param masking
    *   Optional mask with at least `size` elements.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  private def fromIterator[T: ClassTag](
      iterator: Iterator[T],
      size: Int,
      masking: Option[Array[Boolean]] = None,
  ): SeriesData[T] =
    var containsNull = false
    var ix = 0

    val array = new Array[T](size)
    val mask = Array.fill[Boolean](size)(false)
    val maskingArray = masking.orNull

    while ix < size && iterator.hasNext do
      val v = iterator.next
      if maskingArray == null || maskingArray(ix) then
        if v == null then containsNull = true
        else
          array(ix) = v
          mask(ix) = true
      ix = ix + 1

    if ix < size then containsNull = true
    SeriesData(array, if containsNull then mask else null)

  /**
    * Data is checked and copied into an array (allowing T and Null as types).
    *
    * @param iterator
    *   Iterator corresponding to an uniform index.
    * @param size
    *   Number of elements of the `iterator` to process.
    * @param masking
    *   Optional mask with at least `size` elements.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  private def fromIteratorOrNull[T: ClassTag](
      iterator: Iterator[T | Null],
      size: Int,
      masking: Option[Array[Boolean]] = None,
  ): SeriesData[T] =
    var containsNull = false
    var ix = 0

    val array = new Array[T](size)
    val mask = Array.fill[Boolean](size)(false)
    val maskingArray = masking.orNull

    while ix < size && iterator.hasNext do
      val v = iterator.next()
      if maskingArray == null || maskingArray(ix) then
        if v == null then containsNull = true
        else
          array(ix) = v.asInstanceOf[T]
          mask(ix) = true
      ix = ix + 1

    if ix < size then containsNull = true
    SeriesData(array, if containsNull then mask else null)

  /**
    * Option data is copied into an array.
    *
    * @param iterator
    *   Iterator corresponding to an uniform index.
    * @param size
    *   Number of elements of the `iterator` to process.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  private def fromIteratorOption[T: ClassTag](iterator: Iterator[Option[T]], size: Int): SeriesData[T] =
    var containsNull = false
    var ix = 0

    val array = new Array[T](size)
    val mask = Array.fill[Boolean](size)(false)

    while ix < size && iterator.hasNext do
      val v = iterator.next()
      if v != null && v.isDefined then
        val inner = v.get
        if inner != null then
          array(ix) = inner
          mask(ix) = true
        else containsNull = true
      else containsNull = true
      ix = ix + 1

    if ix < size then containsNull = true
    SeriesData(array, if containsNull then mask else null)

  /**
    * Copies data from a SeriesData using a index (to mask the data).
    *
    * @param data
    *   SeriesData.
    * @param size
    *   Number of elements of `data`'s iterator to process.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  private def fromIndex[T](
      data: SeriesData[T],
      index: BaseIndex,
      size: Int,
      masking: Option[Array[Boolean]] = None,
  ): SeriesData[T] =
    var inserted = 0
    val dataVector = data.vector
    val dataMask = data.mask
    val array = data.createArray(size)
    val mask = Array.fill[Boolean](size)(false)
    val maskingArray = masking.orNull

    val it = index.iterator
    while it.hasNext do
      val ix = it.next
      if (ix < size && (dataMask == null || dataMask(ix))) && (maskingArray == null || maskingArray(ix)) then
        val v = dataVector(ix)
        mask(ix) = true
        array(ix) = v
        inserted += 1
    val containsNull = inserted < size
    SeriesData(array, if containsNull then mask else null)

  /**
    * Wraps a DataSeries around an array without copying it.
    *
    * @param array
    *   Array of data that is expected not to be referenced from the outside after this call.
    * @return
    *   SeriesData.
    * @since 0.1.0
    */
  def wrapArray[T](array: Array[T]): SeriesData[T] =
    val clazz = array.getClass
    if clazz.isArray && clazz.getComponentType.isPrimitive then new SeriesData(array)
    else
      val size = array.length
      var containsNull = false
      var ix = 0
      while ix < size do
        if array(ix) == null then
          containsNull = true
          ix = size
        ix = ix + 1

      if containsNull then
        val mask = Array.fill[Boolean](size)(true)
        var ix = 0
        while ix < size do
          if array(ix) == null then mask(ix) = false
          ix = ix + 1
        SeriesData(array, mask)
      else SeriesData(array)
