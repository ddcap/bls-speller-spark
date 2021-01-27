package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.Logging

/**
 * A vector of BLS scores represented as ints
 * @param list Array of BLS scores
 */
@SerialVersionUID(232L)
class BlsVector(var list : Array[Int]) extends Serializable with Logging {
  def addVector(other: BlsVector) = {
    assert(other.list.length == list.length)
    for (i <- 0 until list.length) {
      list(i) += other.list(i)
    }
    this
  }
  def addByte(data: Byte, len: Int) = {
    assert(len == list.length)
    for (i <- 0 to len - 1) {
      list(i) += (if (i<data) 1 else 0);
    }
    this
  }
  def getThresholdCount(idx: Int) = {
    assert(idx < list.size)
    list(idx)
  }

  override
  def toString() : String = {
    return list.mkString("\t");
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
  def getBlsVectorFromByte(data: Byte, len: Int) : BlsVector = {
    var list : Array[Int] = Array.fill(len)(0)
    for (i <- 0 to len - 1) {
      list(i) = (if (i<data) 1 else 0); //  (0x1 & (data >> i));
      // list(i) = (0x1 & (data >> i));
    }
    return new BlsVector(list)
  }
  def getEmptyBlsVector(len: Int) : BlsVector = {
    return new BlsVector(Array.fill(len)(0))
  }
}
