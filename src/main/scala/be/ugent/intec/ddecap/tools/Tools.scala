package be.ugent.intec.ddecap.tools


import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
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
import java.nio.ByteBuffer
import be.ugent.intec.ddecap.dna.BlsVectorFunctions._
import be.ugent.intec.ddecap.dna.{ImmutableDnaPair, ImmutableDnaWithBlsVectorByte, BlsVector, ImmutableDnaWithBlsVector}

@SerialVersionUID(227L)
class Tools(val bindir: String) extends Serializable with Logging {
  type ImmutableDna = Long
  val DegenerationTable = Map(
    'A' -> 1,
    'B' -> 3,
    'C' -> 1,
    'D' -> 3,
    'G' -> 1,
    'H' -> 3,
    'M' -> 2,
    'K' -> 2,
    'N' -> 4,
    'R' -> 2,
    'S' -> 2,
    'T' -> 1,
    'V' -> 3,
    'W' -> 2,
    'Y' -> 2
  )
  val ComplementTable = Map(
    'A' -> 'T',
    'B' -> 'V',
    'C' -> 'G',
    'D' -> 'H',
    'G' -> 'C',
    'H' -> 'D',
    'M' -> 'K',
    'K' -> 'M',
    'N' -> 'N',
    'R' -> 'Y',
    'S' -> 'S',
    'T' -> 'A',
    'V' -> 'B',
    'W' -> 'W',
    'Y' -> 'R'
  )
  def getDegenerationMultiplier(in: String) : Int = {
    // info("motif: " + in)
    var ret = 1
    for (i <- in.size -1 to 0 by -1) {
      ret *= DegenerationTable(in(i))
    }
    ret
  }

  def reverseComplement(in: String) : String = {
    var ret = ""
    for (i <- in.size -1 to 0 by -1) {
      ret += ComplementTable(in(i))
    }
    ret
  }

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

  private def toBinaryFormat(rdd: RDD[String]) : RDD[Array[Byte]] = {
    rdd.map(x => x.getBytes)
  }

  def readOrthologousFamilies(input: String, partitions: Int, sc: SparkContext): RDD[String] = {
    val tmp = sc.wholeTextFiles(input, partitions).flatMap(x => {
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
    // tmp
    tmp.repartition(partitions); //  too many partitions for whole file -> repartition based on size???! of the familiy (# characters)
  }

  def getGeneMap(fasta: String, partitions: Int, sc: SparkContext): RDD[(String, (String, Int, Int))] = {
    val genesToKeep = sc.textFile(fasta, partitions).filter(x => x.startsWith(">")).map(x => {
      val tmp = x.split(":")
      val range = tmp(3).split("-")
      (tmp(0).substring(1), (tmp(2), range(0).toInt + 1, range(1).toInt)) // start is not inclusive, so actually starts one further, the end is inclusive so should be correct
    })
    genesToKeep
  }

  def joinFastaAndLocations(fasta: String, partitions: Int, sc: SparkContext, motifLocations: RDD[((String, Int), (String, Float))], maxMotifLen: Int): RDD[((String, Int), (String, Float))] = {
    val geneMap =  getGeneMap(fasta, partitions, sc)
    info("genes count: " + geneMap.count);

    val geneLocationsInFasta = geneMap.join( motifLocations.map(x => (x._1._1, (x._1._2, x._2) ) ) )
    // returns: [(string, ((string, int,  int), (int,         (string, float))))]
    //            motif     chr     start end    pos_in_gene  (best motif, bls score)
    //            _1      _2._1._1  _2._1._2&3  _2._2._1       2._2._2 (._1&2)
    // filter based on the gene list of that fasta file


    geneLocationsInFasta.map(x => {
      ((x._2._1._1,
        if(x._2._2._1 > 0) x._2._1._2 + x._2._2._1 else x._2._1._3 + x._2._2._1 - (maxMotifLen - 1) + 1), // TODO assumes only a single length of motifs are used!!
        (if(x._2._2._1 > 0) x._2._2._2._1 else reverseComplement(x._2._2._2._1), x._2._2._2._2)
        )
    }).reduceByKey((x, y) => { // merge forward and reverse strand....
       // keeps only highest scoring motif with lowest degenartion (when multiple motifs at max score)
      if(x._2 > y._2)
        x
      else if (x._2 == y._2)
        if (getDegenerationMultiplier(x._1) < getDegenerationMultiplier(y._1)) y else x
      else
        y
    })
  }

  def getCommand(mode: String, alignmentBased: Boolean, thresholdList: List[Float], alphabet: Int, maxDegen: Int, minMotifLen: Int, maxMotifLen: Int, counted : Boolean =  false) : Seq[String] = {
    mode match {
      case "getMotifs" => tokenize( binary + " - " + (if (alignmentBased) AlignmentBasedCommand else AlignmentFreeCommand) + " " + alphabet  + " " +
                           thresholdList.mkString(",") + " " + maxDegen + " " + minMotifLen + " " + maxMotifLen + (if (counted) " true" else ""))
      case "locateMotifs" => tokenize( binary + " - " + (if (alignmentBased) AlignmentBasedCommand else AlignmentFreeCommand) + " " +
                           thresholdList.mkString(",") + " " + maxDegen + " " + maxMotifLen)
      case _ => throw new Exception("invalid Mode")
    }
  }

  def loadMotifs(input: String, partitions: Int, sc: SparkContext, thresholdList: List[Float], fam_cutoff: Int, conf_cutoff:  Double) = {
    val tmp = sc.textFile(input, partitions).flatMap(x => {
      val tmp = x.split("\t")
      var column = 0;
      while(column < thresholdList.size && (tmp(column + 1).toInt < fam_cutoff || tmp(column + 1 + thresholdList.size).toDouble < conf_cutoff)) {
        column+=1;
      }
      if (column < thresholdList.size) Some(tmp(0) + "\t" + column)  else None
    })

    tmp
  }

  def locateMotifs(families: RDD[String], mode: String, motifs: Broadcast[Array[String]], alignmentBased: Boolean,
    maxDegen: Int, maxMotifLen: Int, thresholdList: List[Float]) : RDD[((String, Int), (String, Float))] = {

    val preppedFamiliesAndMotifs = families.map(x => (x + motifs.value.mkString("\n") + "\n"))
    val motifWithLocations = preppedFamiliesAndMotifs.pipe(getCommand(mode, alignmentBased, thresholdList, 0, maxDegen, 0, maxMotifLen))
    val locations = motifWithLocations.flatMap(x => {
      val split = x.split("\t")
      split(2).split(";").map(y => {
        val tmp = y.split("@")
        ((tmp(0), tmp(1).toInt), (split(0), split(1).toFloat))
      })
    }).reduceByKey((x, y) => {
       // keeps only highest scoring motif with lowest degenartion (when multiple motifs at max score)
      if(x._2 > y._2)
        x
      else if (x._2 == y._2)
        if (getDegenerationMultiplier(x._1) < getDegenerationMultiplier(y._1)) y else x
      else
        y
    })
    locations
  }


  // info about data input:
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

  def iterateMotifsOld(input: RDD[String], mode: String, alignmentBased: Boolean, alphabet: Int,
    maxDegen: Int, minMotifLen: Int, maxMotifLen: Int,
    thresholdList: List[Float]) : RDD[(ImmutableDna, ImmutableDnaWithBlsVectorByte)] = {

    (new org.apache.spark.OldBinaryPipedRDD(input, getCommand(mode, alignmentBased, thresholdList, alphabet,
      maxDegen, minMotifLen, maxMotifLen), "motifIterator", maxMotifLen)).flatMap(identity)
  }

  def iterateMotifs(input: RDD[String], mode: String, alignmentBased: Boolean, alphabet: Int,
    maxDegen: Int, minMotifLen: Int, maxMotifLen: Int,
    thresholdList: List[Float]) : RDD[(ImmutableDnaPair, Byte)] = {

    (new org.apache.spark.IteratedBinaryPipedRDD(input, getCommand(mode, alignmentBased, thresholdList, alphabet,
      maxDegen, minMotifLen, maxMotifLen), "motifIterator", maxMotifLen)).flatMap(identity)
  }

  def iterateMotifPairsAndMerge(input: RDD[String], mode: String, alignmentBased: Boolean, alphabet: Int,
    maxDegen: Int, minMotifLen: Int, maxMotifLen: Int,
    thresholdList: List[Float]) : RDD[(ImmutableDnaPair, BlsVector)] = {
    (new org.apache.spark.CountedPairBinaryPipedRDD(input, getCommand(mode, alignmentBased, thresholdList, alphabet,
      maxDegen, minMotifLen, maxMotifLen, true), "motifIterator", maxMotifLen, thresholdList.length))
  }
  def iterateMotifsAndMerge(input: RDD[String], mode: String, alignmentBased: Boolean, alphabet: Int,
    maxDegen: Int, minMotifLen: Int, maxMotifLen: Int,
    thresholdList: List[Float]) : RDD[(ImmutableDna, ImmutableDnaWithBlsVector)] = {
      (new org.apache.spark.CountedBinaryPipedRDD( input , getCommand(mode, alignmentBased, thresholdList, alphabet,
        maxDegen, minMotifLen, maxMotifLen, true), "motifIterator", maxMotifLen, thresholdList.length))
  }

}
