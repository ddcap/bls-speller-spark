package org.apache.spark.util.collection

import be.ugent.intec.ddecap.dna.BlsVector
import be.ugent.intec.ddecap.dna.BlsVectorFunctions._
import scala.reflect.ClassTag
import be.ugent.intec.ddecap.dna.ImmutableDnaPair


// **************************************************************************//
// COPIED FROM OPENHASHMAP (SPARK) AND UPDATED FOR IMMUTABLEDNA -> BLSVECTOR //
// **************************************************************************//

/**
 * A fast hash map implementation for nullable keys. This hash map supports insertions and updates,
 * but not deletions. This map is about 5X faster than java.util.HashMap, while using much less
 * space overhead.
 *
 * Under the hood, it uses our OpenHashSet implementation.
 *
 * NOTE: when using numeric type as the value type, the user of this class should be careful to
 * distinguish between the 0/0.0/0L and non-exist value
 */
// private[spark]
class ImmutableDnaToBlsVectorOpenHashMap(initialCapacity: Int)
  extends Iterable[(ImmutableDnaPair, BlsVector)]
  // extends Iterable[(ImmutableDnaPair, Array[Int])]
  with Serializable {

  def this() = this(64)

  protected var _keySet = new OpenHashSet[ImmutableDnaPair](initialCapacity)

  // Init in constructor (instead of in declaration) to work around a Scala compiler specialization
  // bug that would generate two arrays (one for Object and one for specialized T).
  private var _values: Array[BlsVector] = _
  _values = new Array[BlsVector](_keySet.capacity)

  @transient private var _oldValues: Array[BlsVector] = null

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false
  private var nullValue: BlsVector = null.asInstanceOf[BlsVector]

  override def size: Int = if (haveNullValue) _keySet.size + 1 else _keySet.size

  /** Tests whether this map contains a binding for a key. */
  def contains(k: ImmutableDnaPair): Boolean = {
    if (k == null) {
      haveNullValue
    } else {
      _keySet.getPos(k) != OpenHashSet.INVALID_POS
    }
  }

  /** Get the value for a given key */
  def apply(k: ImmutableDnaPair): BlsVector = {
    if (k == null) {
      nullValue
    } else {
      val pos = _keySet.getPos(k)
      if (pos < 0) {
        null.asInstanceOf[BlsVector]
      } else {
        _values(pos)
      }
    }
  }

  /** Set the value for a key */
  def update(k: ImmutableDnaPair, v: BlsVector): Unit = {
    if (k == null) {
      haveNullValue = true
      nullValue = v
    } else {
      val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
      _values(pos) = v
      _keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(k:ImmutableDnaPair, defaultValue: => BlsVector, mergeValue: (BlsVector) => BlsVector): BlsVector = {
    if (k == null) {
      if (haveNullValue) {
        nullValue = mergeValue(nullValue)
      } else {
        haveNullValue = true
        nullValue = defaultValue
      }
      nullValue
    } else {
      val pos = _keySet.addWithoutResize(k)
      if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = defaultValue
        _values(pos & OpenHashSet.POSITION_MASK) = newValue
        _keySet.rehashIfNeeded(k, grow, move)
        newValue
      } else {
        _values(pos) = mergeValue(_values(pos))
        _values(pos)
      }
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def updateBlsVector(k: ImmutableDnaPair, blvByte: Byte, thresholdListSize: Int) = {
    if (k == null) {
      if (haveNullValue) {
        nullValue = nullValue.addByte(blvByte)
        // for (i <- 0 until blvByte) {
          // nullValue(i) += 1;
        // }
      } else {
        haveNullValue = true
        nullValue =  getBlsVectorFromByte(blvByte, thresholdListSize)
      }
      nullValue
    } else {
      val pos = _keySet.addWithoutResize(k)
      if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = getBlsVectorFromByte(blvByte, thresholdListSize)
        _values(pos & OpenHashSet.POSITION_MASK) = newValue
        _keySet.rehashIfNeeded(k, grow, move)
      } else {
        _values(pos) = _values(pos).addByte(blvByte)
        // for (i <- 0 until blvByte) {
          // _values(pos)(i) += 1;
        // }
      }
    }
  }

  override def iterator: Iterator[(ImmutableDnaPair, BlsVector)] = new Iterator[(ImmutableDnaPair, BlsVector)] {
    var pos = -1
    var nextPair: (ImmutableDnaPair, BlsVector) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating */
    def computeNextPair(): (ImmutableDnaPair, BlsVector) = {
      if (pos == -1) {    // Treat position -1 as looking at the null value
        if (haveNullValue) {
          pos += 1
          return (null.asInstanceOf[ImmutableDnaPair], nullValue)
        }
        pos += 1
      }
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = (_keySet.getValue(pos), _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    def hasNext: Boolean = nextPair != null

    def next(): (ImmutableDnaPair, BlsVector) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[BlsVector](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
