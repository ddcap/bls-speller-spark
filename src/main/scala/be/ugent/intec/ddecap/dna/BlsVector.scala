package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.Logging

/**
 * A vector of BLS scores represented as ints
 * @param list Array of BLS scores
 */
@SerialVersionUID(232L)
class BlsVector(var list: Array[Int]) extends Serializable with Logging {
  /**
   * Return vector of element-wise sum of this and the other vector
   */
  def addVector(other: BlsVector): Array[Int] = {
    assert(other.list.length == list.length)
    (list, other.list).zipped.map(_ + _)
  }

  /**
   * Add 1 to vector element-wise if bit in Byte is 1
   * @param data
   * @param len
   */
  def addByte(data: Byte, len: Int): Unit = {
    assert(len == list.length)
    for (i <- 0 until len) {
      list(i) += (0x1 & (data >> i))
    }
  }

  override
  def toString: String = {
    var ret = list(0).toString
    for (i <- 1 until list.length) {
      ret += "," + list(i)
    }
    ret
  }

  override def hashCode: Int = {
    list.hashCode()
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[BlsVector]

  override def equals(that: Any): Boolean = {
    that match {
      case that: BlsVector => that.canEqual(this) && this.list.deep == that.list.deep
      case _ => false
    }
  }
}

object BlsVectorFunctions {
  def getBlsVectorFromByte(data: Byte, len: Int): BlsVector = {
    val list = new Array[Int](len)
    for (i <- 0 until len) {
      list(i) = 0x1 & (data >> i)
    }
    new BlsVector(list)
  }
}
