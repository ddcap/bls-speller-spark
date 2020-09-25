package be.ugent.intec.ddecap.dna

import scala.collection.mutable.ListBuffer
import be.ugent.intec.ddecap.Logging

@SerialVersionUID(232L)
class BlsVector(var list : ListBuffer[Int]) extends Serializable with Logging {
  def addVector(other: BlsVector) = {
    assert(other.list.length == list.length)
    (list, other.list).zipped.map(_ + _)
  }
  def addByte(data: Byte, len: Int) = {
    assert(len == list.length)
    for (i <- 0 to len - 1) {
      list(i) += (0x1 & (data >> i));
    }
  }
  override
  def toString() : String = {
    var ret = list(0).toString
    for ( i <- 1 to list.length - 1) {
      ret += "," + list(i);
    }
    return ret;
  }
}

object BlsVectorFunctions {
  def getBlsVectorFromByte(data: Byte, len: Short) : BlsVector = {
    var list = ListBuffer[Int]()
    for (i <- 0 to len - 1) {
      list += (0x1 & (data >> i)).toShort;
    }
    return new BlsVector(list)
  }
}
