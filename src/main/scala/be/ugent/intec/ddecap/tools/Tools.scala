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


@SerialVersionUID(227L)
class Tools(val bindir: String) extends Serializable with Logging {
  val mapCommand = bindir + "/suffixtree"

  private def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }

  def readOrthologousFamilies(input: String, sc: SparkContext): RDD[String] = {
    sc.wholeTextFiles(input).map(_._2)
  }

  def iterateMotifs(input: RDD[String]) : RDD[Array[Byte]] = {
    (new org.apache.spark.BinaryPipedRDD(input, tokenize(mapCommand), "motifIterator"))
  }

  def saveMotifs(input: RDD[Array[Byte]]) = {
    
  }

}
