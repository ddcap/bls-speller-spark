package be.ugent.intec.ddecap.dna

import org.apache.log4j._
import scala.collection.mutable.ListBuffer

object LongEncodedDna {
  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  val logger = Logger.getLogger("be.ugent.intec.ddecap.dna.LongEncodedDna");
  type ImmutableDna = Long

  def getDnaLength(data: ImmutableDna): Int = {
    ((data >> 56) & 0xff).toInt
  }

  def getDnaContent(data: ImmutableDna):  ListBuffer[Int] = {
    val len = getDnaLength(data); // first byte is length
    val chars = ListBuffer[Int]()
    ((data >> 56) & 0xff).toInt
    for (i <- 0 until len) {
      // pos of this letter is i << 4*(64-i)
        chars += (data >> (52 - (4*i)) & 0xf).toInt
    }
    chars
  }

  def LongToDnaString(data: ImmutableDna) : String = {
    val len = getDnaLength(data); // first byte is length
    var ret = "";
    for (i <- 0 until len) {
      // pos of this letter is i << 4*(64-i)
        ret += byteToAscii((data >> (52 - (4*i)) & 0xf).toInt)
    }
    ret;
  }
  def LongToDnaString(data: ImmutableDna, len: Int) : String = {
    var ret = "";
    for (i <- 0 until len.toInt) {
      // pos of this letter is i << 4*(64-i)
        ret += byteToAscii((data >> (60 - (4*i)) & 0xf).toInt)
    }
    ret;
  }
}
