package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.Logging
import java.nio.{ByteOrder, ByteBuffer};
import org.apache.log4j.{Level, Logger}

/**
 * A vector of BLS scores represented as ints
 * @param list Array of BLS scores
 */
class BlsVector(var list : Array[Int]) { //Serializable with
  def addVector(other: BlsVector) = {
    // assert(other.list.length == list.length)
    for (i <- 0 until list.length) {
      list(i) += other.list(i)
    }
    this
  }
  def addByte(data: Byte) = {
    // assert(len == list.length)
    for (i <- 0 until data) {
      list(i) += 1; //(if (i<data) 1 else 0);
    }
    this
  }
  def getThresholdCount(idx: Int) = {
    // assert(idx < list.size)
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
  // val logger = Logger.getLogger("be.ugent.intec.ddecap.tools.FileUtils");
  // type BlsVector = Array[Int];

  def getBlsVectorFromByte(data: Byte, len: Int) : BlsVector = {
    var list : Array[Int] = Array.fill(len)(0)
    for (i <- 0 until data) {
      list(i) = 1 // (if (i<data) 1 else 0); //  (0x1 & (data >> i));
      // list(i) = (0x1 & (data >> i));
    }
    return new BlsVector(list)
    // return list
  }
  def getEmptyBlsVector(len: Int) : BlsVector = {
    return new BlsVector(Array.fill(len)(0))
    // return Array.fill(len)(0)
  }
  def getBlsVectorFromShortList(data: Array[Byte], len: Int) : BlsVector = {
    var list : Array[Int] = Array.fill(len)(0)
    val bb = ByteBuffer.wrap(data)
    bb.order(ByteOrder.nativeOrder())
    for (i <- 0 until len) {
      list(i) = bb.getShort(); //  (0x1 & (data >> i));
    }
    return new BlsVector(list)
    // return list
  }
  def getBlsVectorFromCharList(data: Array[Byte], len: Int) : BlsVector = {
    if(len != 5) {throw new Exception("invalid length read!: " + len);}
    var list : Array[Int] = Array.fill(len)(0)
    for (i <- 0 until len) {
      list(i) = data(i) & 0xff; //  (0x1 & (data >> i));
    }
    return new BlsVector(list)
    // return list
  }
}
