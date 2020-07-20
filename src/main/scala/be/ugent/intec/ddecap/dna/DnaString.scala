package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.dna.BlsVectorFunctions._
import scala.collection.mutable.ListBuffer
import be.ugent.intec.ddecap.Logging

@SerialVersionUID(231L)
class DnaString(val len: Short, val data: Array[Byte]) extends Serializable with Logging {
  // def canEqual(a: Any) = a.isInstanceOf[Kmer]
  // override def equals(that: Any): Boolean = {
  //   that match {
  //       case that: Kmer => {
  //           that.canEqual(this) &&
  //           this.canonical == that.canonical
  //       }
  //       case _ => false
  //   }
  // }
  // override def hashCode: Int = {
  //   canonical.hashCode
  // }
  // /**
  // * Define the implicit ordering as the ordering by the canonical string
  // */
  // def compare (that: Kmer) = this.canonical.compare(that.canonical)
}


object DnaStringFunctions {
  val numOfThresholds: Short = 6; // TODO add this to function or some other way to set this
  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  def readBinaryKeyPair(data: Array[Byte]) : ListBuffer[(DnaString, (DnaString, BlsVector))] = {
    var list = ListBuffer[(DnaString, (DnaString, BlsVector))]();
    var i = 0;
    while ( i < data.size) {
      // read size of word:
      val size = data(i);
      val bytesForWord = if (size % 2 == 0) size / 2 else size / 2 + 1;
      i+=1;
      val groupData = data.slice(i, i + bytesForWord);
      i+= bytesForWord;
      val wordData = data.slice(i, i + bytesForWord);
      i+= bytesForWord;
      val vectorData = data(i)
      i+=1;
      list += ((new DnaString(size, groupData), (new DnaString(size, wordData), getBlsVectorFromByte(vectorData, numOfThresholds))))
    }
    return list;
  }

  def dnaToString(str: DnaString) : String = {
    var ret = "";
    for (i <- 0 to str.len - 1) {
      if(i % 2 == 0)
        ret += byteToAscii(str.data(i / 2) & 0xf)
      else
        ret += byteToAscii((str.data(i / 2)  >> 4) & 0xf)
    }
    ret;
  }
}
