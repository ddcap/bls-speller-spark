package be.ugent.intec.ddecap.dna

import org.apache.log4j._

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import be.ugent.intec.ddecap.dna.LongEncodedDna._

object BinaryDnaStringFunctions {

  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  val logger = Logger.getLogger("be.ugent.intec.ddecap.dna.BinaryDnaStringFunctions");
  type ImmutableDna = Long

  def generateBackgroundModel(key: ImmutableDna, backgroundModelCount: Int) : ListBuffer[ImmutableDna] = {
    // generate x permutations of this key motif
    val ret = ListBuffer[ImmutableDna]()
    val chars = getDnaContent(key)
    for (tmp <- 1 to backgroundModelCount) {
      val shuffledIdx = util.Random.shuffle[Int, IndexedSeq](0 until chars.size)
      // random order of chars
      var newdata: Long = 0
      var i = 0
      for (cidx <- shuffledIdx) {
        val c = chars(cidx).toLong
        newdata |= (c << (60 - (4*i))).toLong
        i+= 1
      }
      // logger.info("bg long: " + newdata.toBinaryString)
      ret += newdata;
    }
    ret
  }



  def chooseRandomPivot(arr: List[Int]): Int = arr(scala.util.Random.nextInt(arr.size))
  @tailrec final def findKMedian(arr: List[Int], k: Int): Int = {
      val a = chooseRandomPivot(arr)
      val (s, b) = arr partition (a >)
      if (s.size == k) a
      // The following test is used to avoid infinite repetition
      else if (s.isEmpty) {
          val (s, b) = arr partition (a ==)
          if (s.size > k) a
          else findKMedian(b, k - s.size)
      } else if (s.size < k) findKMedian(b, k - s.size)
      else findKMedian(s, k)
  }
  def findMedian(arr: List[Int]) = findKMedian(arr, (arr.size - 1) / 2)


  def getMedianPerThreshold(data: HashMap[ImmutableDna, BlsVector], bgmodel: ListBuffer[ImmutableDna], thresholdListSize: Int) : BlsVector = {
    // uses the existing motifs with corresponding bls vectors to determine the backgrounmodel, other motifs have a bls vector with all 0s
    val arr = Array.fill(thresholdListSize)(0)
    val nulvector = new BlsVector(Array.fill(thresholdListSize)(0))
    val bgmodelMap = bgmodel.groupBy(identity).map(x => (x._1, x._2.size))
    val bgmodelVectors = ListBuffer[BlsVector]()
    for(d <- data) { // for each motif calculate every F(Ti) and corresponding C(Ti)
      if(bgmodelMap.contains(d._1)) {
        for(i <- 0 until bgmodelMap(d._1)) {
          bgmodelVectors += d._2
        }
      }
    }
    // logger.info("bgmodelVectors.size: " + bgmodelVectors.size);
    for (i <- bgmodelVectors.size until bgmodel.size) {
      bgmodelVectors += nulvector
    }
    for (i <- 0 until thresholdListSize) {
      arr(i) = findMedian(bgmodelVectors.map(x => x.getThresholdCount(i)).toList)
    }
    new BlsVector(arr)
  }

  // def dnaWithoutLenToString(data: ImmutableDna, len: Byte) : String = {
  //   var ret = "";
  //   for (i <- 0 until len) {
  //     if(i % 2 == 0) {
  //       ret += byteToAscii(data(i / 2) & 0xf)
  //     } else {
  //       ret += byteToAscii((data(i / 2)  >> 4) & 0xf)
  //     }
  //   }
  //   ret;
  // }
  // def dnaToString(data: ImmutableDna) : String = {
  //   val len = data(0);
  //   var ret = "";
  //   for (i <- 0 until len) {
  //     if(i % 2 == 0) {
  //       ret += byteToAscii(data(1+i / 2) & 0xf)
  //     } else {
  //       ret += byteToAscii((data(1+i / 2)  >> 4) & 0xf)
  //     }
  //   }
  //   ret;
  // }

  // def stringToDNA(data: String): ImmutableDna = {
  //   val output = new Array[Byte](1 + (data.length.toDouble / 2).round.toInt)
  //   // Add length string to start
  //   output(0) = data.length.toByte
  //   var current_byte : Byte = 0x0
  //   var i = 0
  //   for (char <- data) {
  //     val index = byteToAscii.indexOf(char)
  //     if (i % 2 == 0) {
  //       // char_index = bytes (>> 4) & 0xf
  //       // bytes =
  //       current_byte = index.toByte
  //     } else {
  //       current_byte = (current_byte.toInt | index.toByte << 4).toByte
  //       output(1+ (i-1) / 2) = current_byte
  //     }
  //     // add last unfinished byte if present
  //     if (i % 2 == 0) {
  //       output(1+ i / 2) = current_byte
  //     }
  //     i += 1
  //   }
  //   output.toVector
  // }

}
