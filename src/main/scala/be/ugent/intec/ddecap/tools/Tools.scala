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
  val binary = bindir + "/motifIterator"
  // TODO add options to the tool 3 degen and length range  6- 13, is hard coded right now
  val AlignmentBasedCommand = binary + " AB - "
  val AlignmentFreeCommand = binary + " AF - "

  private def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
  private def toBinaryFormat(rdd: RDD[String]) : RDD[List[Byte]] = {
    rdd.map(x => x.getBytes.toList)
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
    tmp.repartition(tmp.count.toInt); //  too many partitions for whole file -> repartition based on size???! of the familiy (# characters)
  }

  def getCommand(alignmentBased: Boolean, maxDegen: Int, minMotifLen: Int, maxMotifLen: Int) : Seq[String] = {
    tokenize( if (alignmentBased) AlignmentBasedCommand else AlignmentFreeCommand + " " + maxDegen + " " + minMotifLen + " " + maxMotifLen)
  }

  def iterateMotifs(input: RDD[String], alignmentBased: Boolean, maxDegen: Int, minMotifLen: Int, maxMotifLen: Int, partitions: Int, thresholdCount: Int) : RDD[(List[Byte], BlsVector)] = {
    (new org.apache.spark.BinaryPipedRDD(toBinaryFormat(input), getCommand(alignmentBased, maxDegen, minMotifLen, maxMotifLen), "motifIterator", (maxMotifLen >> 1) + 2 ))
        .repartition(partitions)
        .map(x => splitBinaryDataInMotifAndBlsVector(x, (maxMotifLen >> 1) + 2 ))
        .aggregateByKey(new BlsVector(ListBuffer.fill(thresholdCount)(0)))(
            seqOp = (v, b) => {
              v.addByte(b, thresholdCount)
              v
            },
            combOp = (v1, v2) => {
              v1.addVector(v2)
              v1
            })
  }

  def groupMotifsByGroup(input: RDD[(List[Byte], BlsVector)], maxMotifLen: Int) : RDD[(List[Byte], ListBuffer[(List[Byte], BlsVector)])] = {
    input.map(x => {
                (getGroupId(x._1, (maxMotifLen >> 1)  + 1), (x._1, x._2)) //length is now without the bls vector byte
              }).aggregateByKey(new ListBuffer[(List[Byte], BlsVector)]())(
                  seqOp = (list, element) => {
                    list += element
                  },
                  combOp = (list1, list2) => {
                    list1 ++ list2
                  })
  }

  def processGroups(input: RDD[(DnaString, ListBuffer[(DnaString, BlsVector)])],
      thresholdList: List[Float],
      backgroundModelCount: Int, familyCountCutOff: Int, confidenceScoreCutOff: Float) : RDD[Int] = {
    input.mapPartitions(x => {
      // x is an iterator over the motifs+blsvector in this group
      // TODO for every Threshold Ti:
        // TODO calculate backgroundModel and F_BG(Ti)

        // TODO for each motif calculate every F(Ti) and corresponding C(Ti)


        // TODO emit motifs with their Ti and corresponding F(Ti) and C(Ti)
      List(x.size).iterator
    })
  }

  def stringRepresentation(input: RDD[(DnaString, (DnaString, BlsVector))]) : RDD[String] = {
    input.map(x => (dnaToString(x._1) + "\t" + dnaToString(x._2._1) + "\t" + x._2._2.toString()))
  }

  def saveMotifs(input: RDD[Array[Byte]]) = {

  }

}
