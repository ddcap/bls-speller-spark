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

  def getDnaContent(data: ImmutableDna):  ListBuffer[Long] = {
    val len = getDnaLength(data); // first byte is length
    val chars = ListBuffer[Long]()
    for (i <- 0 until len) {
      // pos of this letter is i << 4*(64-i)
        chars += (data >> (52 - (4*i)) & 0xf) // this has length:  // 52 here since no length 60 - 8
    }
    chars
  }

  def randomDnaStringWithLength(length: Int) : Long = {
    assert(length < 15);
    val rnd = new scala.util.Random
    var newdata: Long = 0
    for (i <- 0 until length) {
      val c = 1 + rnd.nextInt(15).toLong
      newdata |= (c << (52 - (4*i))).toLong
    }
    newdata | (length.toLong << 56)
  }
  def randomDnaString(length: Int) : Long = {
    assert(length < 15);
    val rnd = new scala.util.Random
    var newdata: Long = 0
    for (i <- 0 until length) {
      val c = 1 + rnd.nextInt(15).toLong
      newdata |= (c << (60 - (4*i))).toLong
    }
    newdata;
  }

  def LongToDnaString(data: ImmutableDna) : String = {
    val len = getDnaLength(data); // first byte is length
    var ret = "";
    for (i <- 0 until len) {
      // pos of this letter is i << 4*(64-i)
         ret += byteToAscii((data >> (52 - (4*i)) & 0xf).toInt) // 52 here since no length 60 - 8
    }
    ret;
  }
  def LongToDnaString(data: ImmutableDna, len: Int) : String = {
    var ret = "";
    for (i <- 0 until len.toInt) {
      // pos of this letter is i << 4*(64-i)
        ret += byteToAscii((data >> (60 - (4*i)) & 0xf).toInt) // 60 here since no length
    }
    ret;
  }
}
