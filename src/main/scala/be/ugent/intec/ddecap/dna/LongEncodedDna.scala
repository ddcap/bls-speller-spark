package be.ugent.intec.ddecap.dna

import org.apache.log4j._
import scala.collection.mutable.ListBuffer

object LongEncodedDna {
  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  private final val asciiToByte = Map('A'->1l, 'C'->2l, 'M'->3l, 'G'->4l, 'R'->5l, 'S'->6l, 'V'->7l, 'T'->8l, 'W'->9l, 'Y'->10l, 'H'->11l, 'K'->12l, 'D'->13l, 'B'->14l, 'N'->15l)
  private final val complement = Map(
    'A'-> 'T', 'C'-> 'G', 'G'-> 'C', 'T'-> 'A',
    'N'-> 'N', 'R'-> 'Y', 'Y'-> 'R', 'S'-> 'S',
    'W'-> 'W', 'K'-> 'M', 'M'-> 'K', 'B'-> 'V',
    'V'-> 'B', 'D'-> 'H', 'H'-> 'D');
  val logger = Logger.getLogger("be.ugent.intec.ddecap.dna.LongEncodedDna");
  type ImmutableDna = Long
  def isRepresentative(motif: ImmutableDna, length: Int) : Boolean = {
      val motifstr = LongToDnaString(motif, length);
      var i = 0
      while (i < length && motifstr(i) == complement(motifstr(length - 1 - i))) { i+=1; }
      // if the same, ie i == read.length -> RC wont be matched in the tree since it is the same!!1 so no need to save them to check if it already has passed in the motifs.
      // logger.info("motif\t"  + motifstr  + "\t" + ( i == length || motifstr(i) < complement(motifstr(length - 1 - i))))
      return i == length || motifstr(i) < complement(motifstr(length - 1 - i));
  }
  def reverseComplement(motif: String) : String = {
    val length = motif.length
    var rc = ""
    var i = 0
    while (i < length) {
      rc = complement(motif(i)) + rc
      i+=1
    }
    rc
  }
  def reverseComplement(motif: ListBuffer[Char]) : ListBuffer[Char] = {
    val length = motif.length
    var rc:ListBuffer[Char] = ListBuffer.empty[Char];
    var i = 0
    while (i < length) {
      rc += complement(motif(i))
      i+=1
    }
    rc
  }
  def isGroupItsOwnRc(data: ImmutableDna) : Boolean = { // need to use ascii since sorting in binary is different than ascii!
    val len = getDnaLength(data); // first byte is length
    var groupcontent :ListBuffer[Char] = ListBuffer.empty[Char];
    for (i <- 0 until len) {
       groupcontent += byteToAscii((data >> (52 - (4*i)) & 0xf).toInt) 
    }
    val RC = reverseComplement(groupcontent).sorted;
    // logger.info("group\t"  + groupstr  + "\t" + RC + "\t" + groupstr == RC)
    return groupcontent.mkString == RC.mkString;
  }


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
  def DnaStringToLongWithLength(motif: String) : Long = {
    val length = motif.length
    var newdata: Long = 0
    for (i <- 0 until length) {
      val c = asciiToByte(motif(i))
      newdata |= (c << (52 - (4*i))).toLong
    }
    newdata | (length.toLong << 56)
  }

  def DnaStringToLong(motif: String) : Long = {
    val length = motif.length
    var newdata: Long = 0
    for (i <- 0 until length) {
      val c = asciiToByte(motif(i))
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
