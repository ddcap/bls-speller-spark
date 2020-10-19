package be.ugent.intec.ddecap.tools


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel
import be.ugent.intec.ddecap.Logging
import java.util.StringTokenizer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.BinaryPipedRDD

@SerialVersionUID(227L)
class Tools(val bindir: String) extends Serializable with Logging {
  type ContentWithMotifAndBls = (Array[Byte], (Array[Byte], Byte)) // TODO replace this where possible

  val binary = bindir + "/motifIterator"
  // TODO add options to the tool 3 degen and length range  6- 13, is hard coded right now
  val AlignmentBasedCommand = " AB "
  val AlignmentFreeCommand = " AF "

  private def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
  private def toBinaryPairFormat(rdd: RDD[String]) : RDD[(Seq[Byte], (Seq[Byte], Byte))] = {
    rdd.map(x => (x.getBytes, (Seq(), 0x0)))
  }
  private def toBinaryFormat(rdd: RDD[String]) : RDD[Array[Byte]] = {
    rdd.map(x => x.getBytes)
  }

  def readOrthologousFamilies(input: String, partitions: Int, sc: SparkContext): RDD[String] = {
    val tmp = sc.wholeTextFiles(input).flatMap(x => {
      val tmp = x._2.split("\n");
      val list: ListBuffer[String] = new ListBuffer[String]();
      // split per ortho family
      var i = 0;
      while (i < tmp.size) {
        var ortho = "";
        while(tmp(i).isEmpty()) {i+=1;}
        ortho += tmp(i) + "\n"; // name
        i+=1;
        ortho += tmp(i) + "\n"; // newick
        i+=1;
        val N = tmp(i).toInt;
        ortho += tmp(i) + "\n"; // Count
        i+=1;
        for(j <- 0 until N) {
          ortho += tmp(i) + "\n"; // ortho name
          i+=1;
          ortho += tmp(i) + "\n"; // DNA string
          i+=1;
        }
        list.append(ortho);
      }
      list
    })
    tmp.repartition(partitions); //  too many partitions for whole file -> repartition based on size???! of the familiy (# characters)
  }

  def getCommand(alignmentBased: Boolean, thresholdList: List[Float], alphabet: Int, maxDegen: Int, minMotifLen: Int, maxMotifLen: Int) : Seq[String] = {
    tokenize( binary + " - " + (if (alignmentBased) AlignmentBasedCommand else AlignmentFreeCommand) + " " + alphabet  + " " + thresholdList.mkString(",") + " " + maxDegen + " " + minMotifLen + " " + maxMotifLen)
  }

  def iterateMotifs(input: RDD[String], alignmentBased: Boolean, alphabet: Int,
    maxDegen: Int, minMotifLen: Int, maxMotifLen: Int,
    thresholdList: List[Float]) : RDD[(Seq[Byte], (Seq[Byte], Byte))] = {

      // iterateMotifs (c++ binary) outputs binary data, per motif this content is given:
      // 1 byte: length of motif
      // x bytes: motif content group in binary format, length depends on first byte (length) where there's 2 characters per byte
      // x bytes: motif itself in binary format
      // 1 byte: bls vector, first bit is 1 if the bls sscore of this motif in this family is higher then the first threshold, and so on for up to 8 thresholds
      // binary format:
        // 2 charactes per byte:
        //    4 bits per character:   T G C A
        //                            x x x x -> 1 if that letter is in the iupac letter, 0 if not
        //                      ie A: 0 0 0 1
        //                      ie G: 0 1 0 0
        //                      ie M: 0 0 1 1 // A or C

    // this is formatted in a key value pair as follows:
    // key: array[byte] -> first byte of length + motif content group
    // value: (array[byte], byte) -> the content of the motif itself (without the length! as this is already in the key) + the bls byte
    (new org.apache.spark.BinaryPipedRDD(toBinaryPairFormat(input), getCommand(alignmentBased, thresholdList, alphabet, maxDegen, minMotifLen, maxMotifLen), "motifIterator", maxMotifLen))
  }

}
