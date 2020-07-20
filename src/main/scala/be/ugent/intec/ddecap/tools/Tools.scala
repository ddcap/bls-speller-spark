package be.ugent.intec.ddecap.tools


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import be.ugent.intec.ddecap.Logging
import java.util.StringTokenizer
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.BinaryPipedRDD
import be.ugent.intec.ddecap.dna.{DnaString, BlsVector}
import be.ugent.intec.ddecap.dna.DnaStringFunctions._


@SerialVersionUID(227L)
class Tools(val bindir: String) extends Serializable with Logging {
  val binary = bindir + "/suffixtree"
  // TODO add options to the tool 3 degen and length range  6- 13, is hard coded right now
  val AlignmentBasedCommand = binary + " AB - 3 6 13"
  val AlignmentFreeCommand = binary + " AF - 3 6 13"

  private def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
  private def toBinaryFormat(rdd: RDD[String]) : RDD[Array[Byte]] = {
    rdd.map(x => x.getBytes)
  }

  def readOrthologousFamilies(input: String, sc: SparkContext): RDD[String] = {
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
        for(j <- 1 to N) {
          ortho += tmp(i) + "\n"; // ortho name
          i+=1;
          ortho += tmp(i) + "\n"; // DNA string
          i+=1;
        }
        list.append(ortho);
      }
      list
    })
    tmp.repartition(tmp.count.toInt);
  }

  def iterateMotifs(input: RDD[String], alignmentBased: Boolean, partitions: Int) : RDD[(DnaString, (DnaString, BlsVector))] = {
    (new org.apache.spark.BinaryPipedRDD(toBinaryFormat(input), tokenize( if (alignmentBased) AlignmentBasedCommand else AlignmentFreeCommand), "motifIterator"))
        .flatMap(x => readBinaryKeyPair(x))
        // .repartition(partitions)
  }

  def stringRepresentation(input: RDD[(DnaString, (DnaString, BlsVector))]) : RDD[String] = {
    input.map(x => (dnaToString(x._1) + "\t" + dnaToString(x._2._1) + "\t" + x._2._2.toString()))
  }

  def saveMotifs(input: RDD[Array[Byte]]) = {

  }

}
