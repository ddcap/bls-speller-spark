package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.dna.BlsVectorFunctions._
import scala.collection.mutable.ListBuffer
import be.ugent.intec.ddecap.Logging
import org.apache.log4j._

@SerialVersionUID(231L)
case class DnaString(val len: Short, val data: Array[Byte])

object DnaStringFunctions {

  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  val logger = Logger.getLogger(getClass().getName());

  def readBinaryKeyPairs(data: Array[Byte]) : ListBuffer[((DnaString, DnaString), Byte)] = {
     var list = ListBuffer[((DnaString, DnaString), Byte)]();
     var i = 0;
     var wordidx = 0
     var groupidx = 0
     while ( i < data.size) {
       // read size of word:
       val size = data(i);
       val bytesForWord =  (size >> 1) + (size & 0x1);
       i+=1;
       wordidx = i
       groupidx = i + bytesForWord
       i+= 2*bytesForWord + 1;
       // logger.info("dnastring; " + dnaToString(new DnaString(size, wordData)))
       // TODO this is not correct -> check the indexes
       list += (((DnaString(size, data.slice(i- 2*bytesForWord, i)), DnaString(size, data.slice(i-bytesForWord, i ))), data(i-1)))
     }
     logger.info("listsize: "  + list.size)
     return list;
   }
   def splitBinaryDataInMotifAndBlsVector(data: Array[Byte], wordSize: Int) : (List[Byte], Byte) = {
     val blsvector = data(wordSize - 1);
     (data.dropRight(1).toList, blsvector)
   }
   def getGroupId(data: List[Byte], wordSize: Int) : List[Byte] = {
     val newdata = new Array[Byte](wordSize)
     newdata(0) = data(0)
     val chars = ListBuffer[Int]()
     for (d <- 0 to data(0) - 1 ) {
       if((d & 0x1) == 0)
         chars += (data(d >> 1) & 0xf)
       else
         chars += ((data(d >> 1)>> 4) & 0xf)
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
   def readBinaryKeyPair(data: Array[Byte]) : ((DnaString, DnaString), Byte) = {
      var i = 0;
      val size = data(i);
      val bytesForWord = (size >> 1) + (size & 0x1);
      i+=1;
      val groupData = data.slice(i, i + bytesForWord);
      i+= bytesForWord;
      val wordData = data.slice(i, i + bytesForWord);
      i+= bytesForWord;
      val vectorData = data(i)
      i+=1;
      // logger.info("dnastring; " + dnaToString(new DnaString(size, wordData)))
     ((DnaString(size, groupData), DnaString(size, wordData)), vectorData)
    }


  def dnaToString(str: DnaString) : String = {
    var ret = "";
    for (i <- 0 to str.len - 1) {
      if(i % 2 == 0) {
        ret += byteToAscii(str.data(i / 2) & 0xf)
      } else {
        ret += byteToAscii((str.data(i / 2)  >> 4) & 0xf)
      }
    }
    ret;
  }
}
