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
  // type BlsVector = Array[Int];
  private val similarityCountfactor = 5;

  def generateBackgroundModel(key: ImmutableDna, backgroundModelCount: Int, similarityScore: Int): ListBuffer[ImmutableDna] = {
    similarityScore match {
      case 0 => {
        generateDissimilarBackgroundModel(key, backgroundModelCount, binaryHammingSimilarityScore)
      }
      case 1 => {
        generateDissimilarBackgroundModel(key, backgroundModelCount, hammingSimilarityScore)
      }
      case 2 => {
        generateDissimilarBackgroundModel(key, backgroundModelCount, levenshteinSimilarityScore)
      }
      case _ => {
        generateBackgroundModel(key, backgroundModelCount)
      }
    }
  }

  private def generateDissimilarBackgroundModel(key: ImmutableDna, backgroundModelCount: Int, similarityScore: SimilarityScoreType => Long) : ListBuffer[ImmutableDna] = {
    val bgmodel = generateBackgroundModel(key, similarityCountfactor*backgroundModelCount)
    val orderedbgmodel = orderByDissimilarityScore(bgmodel, getDnaLength(key), similarityScore)
    orderedbgmodel.take(backgroundModelCount)
  }


  private def fact(n: Int) : Int = {
    var f = 1
    for (i <- 2 to n) {
      f = f * i
    }
    return f
  }

  private final val bgCountFactor = 4
  private final val bgCountMinimum = 1000
  private def getNumberOfPermutations(chars: ListBuffer[Long]): Int = {
    val charcounts = chars.groupBy(identity).map(x => (x._1, x._2.size))
    var tmp = 1
    for (c <- charcounts) {
    	tmp = tmp * fact(c._2)
    }
    return fact(chars.size)/tmp
  }
  private def getBgCountBasedOnNumberOfPermutations(chars: ListBuffer[Long]): Int = {
    val permutations = getNumberOfPermutations(chars)
    return Math.max(bgCountMinimum,bgCountFactor*permutations)
  }

  private def generateBackgroundModel(key: ImmutableDna, backgroundModelCount: Int) : ListBuffer[ImmutableDna] = {
    // generate x permutations of this key motif
    val ret = ListBuffer[ImmutableDna]()
    val chars = getDnaContent(key)
    val bgcount = if(backgroundModelCount > 0) backgroundModelCount else (getBgCountBasedOnNumberOfPermutations(chars))
    for (tmp <- 0 until bgcount) {
      val shuffledIdx = util.Random.shuffle[Int, IndexedSeq](0 until chars.size)
      // random order of chars
      var newdata: Long = 0
      var i = 0
      for (cidx <- shuffledIdx) {
        val c = chars(cidx)
        newdata |= (c << (60 - (4*i))).toLong // 60 here since no length
        i+= 1
      }
      // logger.info("bg long: " + newdata.toBinaryString)
      ret += newdata;
    }
    ret
  }

  def bitCount(x: Long) : Long =  {
    var i = x
    i - ((i >>> 1) & 0x5555555555555555L);
    i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
    i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
    i = i + (i >>> 8);
    i = i + (i >>> 16);
    i = i + (i >>> 32);
    return i & 0x7f;
  }
  def binaryHammingSimilarityScore(motifdata: SimilarityScoreType) : Long = {
    var ret = bitCount((motifdata.motifa^motifdata.motifb) >> ((16-motifdata.len)<<2)) // shift to right in order to eliminate problems with leftover 1s in that part
    ret.toInt
  }
  def levenshteinSimilarityScore(motifdata: SimilarityScoreType) : Long = {
    var a = (0 until motifdata.len).toList
    var b = (0 until motifdata.len).toList

    ((0 to b.size).toList /: a)((prev, x) =>
     (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
         case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (((motifdata.motifa >> (60 -4*x)) & 0xf) == ((motifdata.motifb >> (60 -4*y)) & 0xf)) 0 else 1))
       }).last
  }
  def hammingSimilarityScore(motifdata: SimilarityScoreType) : Long = {
    var count = 0L
    var xor = (motifdata.motifa ^ motifdata.motifb) >> ((16-motifdata.len)<<2);
    for (i <- 0 until motifdata.len) {
      if ( (xor & 0xf) > 0) {
        count += 1
      }
      xor = xor >> 4;
    }
    count
  }

  def orderByDissimilarityScore(bgmodel: ListBuffer[ImmutableDna], motiflen: Int, similarityScore: SimilarityScoreType => Long) : ListBuffer[ImmutableDna] = {
    val tmp = bgmodel.map(x => {
      val s : Double = bgmodel.map(y => similarityScore(SimilarityScoreType(x,y, motiflen))).sum
      (x, s/bgmodel.size)
    })
    tmp.sortBy(-_._2).map(_._1)
  }



  def chooseRandomPivot(arr: Seq[Int]): Int = arr(scala.util.Random.nextInt(arr.size))
  @tailrec final def findKMedian(arr: Seq[Int], k: Int): Int = {
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
  def findMedian(arr: Seq[Int]) = findKMedian(arr, (arr.size - 1) / 2)

  def getMedianPerThreshold(key: ImmutableDna, data: List[ImmutableDnaWithBlsVector], thresholdListSize: Int) : BlsVector = {
    val chars = getDnaContent(key)
    val perms = getNumberOfPermutations(chars)
    if(perms == 1) return data(0).vector
    else {
      val nr = (perms + 1) / 2
      val arr = Array.fill(thresholdListSize)(0)
      if(data.size > nr) {
        val k = data.size - nr - 1
        for (i <- 0 until thresholdListSize) {
          arr(i) = findKMedian(data.map(x => x.vector.getThresholdCount(i)), k)
        }
      }
      new BlsVector(arr)
    }
  }

  def getMedianPerThreshold(data: List[ImmutableDnaWithBlsVector], bgmodel: ListBuffer[ImmutableDna], thresholdListSize: Int) : BlsVector = {
    // uses the existing motifs with corresponding bls vectors to determine the backgrounmodel, other motifs have a bls vector with all 0s
    val arr = Array.fill(thresholdListSize)(0)
    val nulvector = new BlsVector(Array.fill(thresholdListSize)(0))
    val bgmodelMap = bgmodel.groupBy(identity).map(x => (x._1, x._2.size)) // just a map to get contains function, can be set as well?
    val bgmodelVectors = ListBuffer[BlsVector]()

    for(d <- data) { // loop over smallest one? -> data (contains found motifs) or bgmodelmap (will contain random motifs not in data)
      if(bgmodelMap.contains(d.motif)) {
        for(i <- 0 until bgmodelMap(d.motif)) {
          bgmodelVectors += d.vector
        }
      }
    }
    for (i <- bgmodelVectors.size until bgmodel.size) {
      bgmodelVectors += nulvector
    }
    for (i <- 0 until thresholdListSize) {
      arr(i) = findMedian(bgmodelVectors.map(x => x.getThresholdCount(i)))
    }
    new BlsVector(arr)
  }

  def oldGetMedianPerThreshold(data: HashMap[ImmutableDna, BlsVector], bgmodel: ListBuffer[ImmutableDna], thresholdListSize: Int) : BlsVector = {
    // uses the existing motifs with corresponding bls vectors to determine the backgrounmodel, other motifs have a bls vector with all 0s
    val arr = Array.fill(thresholdListSize)(0)
    val nulvector = new BlsVector(Array.fill(thresholdListSize)(0))
    val bgmodelMap = bgmodel.groupBy(identity).map(x => (x._1, x._2.size))
    val bgmodelVectors = ListBuffer[BlsVector]()
    for(d <- data) { // loop over smallest one? -> data (contains found motifs) or bgmodelmap (will contain random motifs not in data)
      if(bgmodelMap.contains(d._1)) {
        for(i <- 0 until bgmodelMap(d._1)) {
          bgmodelVectors += d._2
        }
      }
    }
    for (i <- bgmodelVectors.size until bgmodel.size) {
      bgmodelVectors += nulvector
    }
    for (i <- 0 until thresholdListSize) {
      arr(i) = findMedian(bgmodelVectors.map(x => x.getThresholdCount(i)))
    }
    new BlsVector(arr)
  }

}
