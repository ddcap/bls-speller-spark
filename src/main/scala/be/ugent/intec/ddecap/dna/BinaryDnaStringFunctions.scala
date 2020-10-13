package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.dna.BlsVectorFunctions._
import scala.collection.mutable.ListBuffer
import org.apache.log4j._
import collection.immutable.HashMap
import scala.annotation.tailrec

object BinaryDnaStringFunctions {

  private final val byteToAscii = Array(' ', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N')
  val logger = Logger.getLogger("be.ugent.intec.ddecap.dna.BinaryDnaStringFunctions");

  def generateBackgroundModel(key: Seq[Byte], backgroundModelCount: Int) : ListBuffer[Seq[Byte]] = {
    // generate x permutations of this key motif
    val ret = ListBuffer[Seq[Byte]]()
    val chars = ListBuffer[Int]()
    for (d <- 0 until key(0) ) {
     if((d & 0x1) == 0)
       chars += (key(1 + (d >> 1)) & 0xf)
     else
       chars += ((key(1 + (d >> 1))>> 4) & 0xf)
    }
    for (tmp <- 1 to backgroundModelCount) {
      val shuffledIdx = util.Random.shuffle[Int, IndexedSeq](0 until chars.size)
      // random order of chars
      val newdata = new Array[Byte](key.size - 1)
      // newdata(0) = key(0) // in the motifs the size isn't recorded as this is present in the key...
      var idx = 0
      var i = 0
      for (cidx <- shuffledIdx) {
        val c = chars(cidx)
       if ((i&1) == 0) {
         newdata(idx) = c.toByte
       } else {
         newdata(idx) = (newdata(idx) | (c << 4)).toByte
         idx += 1
       }
       i+= 1
      }
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


  def getMedianPerThreshold(data: HashMap[Seq[Byte], BlsVector], bgmodel: ListBuffer[Seq[Byte]], thresholdListSize: Int) : BlsVector = {
    // uses the existing motifs with corresponding bls vectors to determine the backgrounmodel, other motifs have a bls vector with all 0s
    val arr = Array.fill(thresholdListSize)(0)
    val nulvector = new BlsVector(Array.fill(thresholdListSize)(0))
    val bgmodelMap = bgmodel.groupBy(identity).map(x => (x._1, x._2.size))
    val bgmodelVectors = ListBuffer[BlsVector]()
    for(d <- data) { // for each motif calculate every F(Ti) and corresponding C(Ti)
      if(bgmodelMap.contains(d._1)) {
      // logger.info("adding " + bgmodelMap(d._1) + " of " + d._2);
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

  def dnaWithoutLenToString(data: Seq[Byte], len: Byte) : String = {
    var ret = "";
    for (i <- 0 until len) {
      if(i % 2 == 0) {
        ret += byteToAscii(data(i / 2) & 0xf)
      } else {
        ret += byteToAscii((data(i / 2)  >> 4) & 0xf)
      }
    }
    ret;
  }
  def dnaToString(data: Seq[Byte]) : String = {
    val len = data(0);
    var ret = "";
    for (i <- 0 until len) {
      if(i % 2 == 0) {
        ret += byteToAscii(data(1+i / 2) & 0xf)
      } else {
        ret += byteToAscii((data(1+i / 2)  >> 4) & 0xf)
      }
    }
    ret;
  }
}
