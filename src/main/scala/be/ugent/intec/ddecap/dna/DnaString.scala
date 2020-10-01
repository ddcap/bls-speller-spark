package be.ugent.intec.ddecap.dna

import org.apache.log4j._

import scala.collection.mutable.ListBuffer

object DnaStringFunctions {

  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  val logger = Logger.getLogger(getClass.getName);

  def splitBinaryDataInMotifAndBlsVector(data: Array[Byte], maxMotifLen: Int) : (List[Byte], Byte) = {
    val blsvector = data(((maxMotifLen >> 1) + 2) - 1);
    (data.dropRight(1).toList, blsvector)
  }

  def getGroupId(data: List[Byte], maxMotifLen: Int) : List[Byte] = {
    val wordSize = (maxMotifLen >> 1)  + 1
    val newdata = new Array[Byte](wordSize)
    newdata(0) = data.head
    val chars = ListBuffer[Int]()
    for (d <- 0 until data.head) {
      if((d & 0x1) == 0) {
        chars += (data(1 + (d >> 1)) & 0xf)
      } else {
        chars += ((data(1 + (d >> 1)) >> 4) & 0xf)
      }
    }
    var idx = 1
    var i = 0
    for (c <- chars.sortBy(x => x)) {
      if ((i&1) == 0) {
        newdata(idx) = c.toByte
      } else {
        newdata(idx) = (newdata(idx) | (c << 4)).toByte
        idx += 1
      }
      i+= 1
    }
    newdata.toList;
  }


  def dnaToString(data: Array[Byte]) : String = {
    // TODO length as first element instead of data.length?
    // TODO usage dnaToString?
    val len = data(0);
    var ret = "";
    for (i <- 0 until len) {
      if(i % 2 == 0) {
        // 0xf == 0b1111
        ret += byteToAscii(data(1+i / 2) & 0xf)
      } else {
        ret += byteToAscii((data(1+i / 2)  >> 4) & 0xf)
      }
    }
    ret;
  }
}
