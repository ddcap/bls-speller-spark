package be.ugent.intec.ddecap.dna

import scala.collection.mutable.ListBuffer
import be.ugent.intec.ddecap.Logging

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
    for (i <- 0 until len) {
      list(i) += (0x1 & (data >> i));
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
}

object BlsVectorFunctions {
  def getBlsVectorFromByte(data: Byte, len: Int) : BlsVector = {
    var list = Array.fill(len)(0)
    for (i <- 0 to len - 1) {
      list(i) = (0x1 & (data >> i));
    }
    return new BlsVector(list)
  }
}
