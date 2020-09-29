package be.ugent.intec.ddecap

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import java.io._
import scala.io.Source
import java.util.zip._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf;
import be.ugent.intec.ddecap.tools.Tools;
import be.ugent.intec.ddecap.rdd.BinaryRDDFunctions._;
import be.ugent.intec.ddecap.tools.FileUtils._;


object BlsSpeller extends Logging {
  case class Config(
      input: String = "",
      partitions: Int = 8,
      alignmentBased: Boolean = false,
      bindir: String = "",
      output: String = "",
      maxDegen: Int = 4,
      minMotifLen: Int = 8,
      maxMotifLen: Int = 9,
      alpbabet: Int = 1, // 0: exact, 1: exact+M, 2: exact+2+M, 3: All
      familyCountCutOff: Int = 1,
      backgroundModelCount: Int = 1000,
      confidenceScoreCutOff: Double = 0.5,
      thresholdList: List[Float] = List(0.15f, 0.5f, 0.6f, 0.7f, 0.9f, 0.95f),
      persistLevel: StorageLevel = StorageLevel.DISK_ONLY
    )

  var sc: SparkContext = null

  def main(args: Array[String]) {

    val optionConfig = parseOptionsAndStart(args)
    optionConfig match {
      case Some(config) =>
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        var conf: SparkConf = null;
        conf = new SparkConf();
        val spark = SparkSession.builder
                                .appName("BLS Speller")
                                .config(conf)
                                .getOrCreate()
        sc = spark.sparkContext

        info("serializer is " + sc.getConf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer"))
        runPipeline(config)
        sc.stop()
        spark.stop()
      case None => throw new Exception("arguments missing")
    }
  }

  def parseOptionsAndStart(args: Array[String]) : Option[Config] = {
    val parser = new scopt.OptionParser[Config]("bls speller") {
      head("BLS Speller", "0.1")

      opt[String]('i', "input").action( (x, c) =>
        c.copy(input = x) ).text("Location of the input files.").required()

      opt[String]('o', "output").action( (x, c) =>
        c.copy(output = x) ).text("The output directory (spark output) or VCF output file (merged into a single file).").required()

      opt[String]('b', "bindir").action( (x, c) =>
        c.copy(bindir = x) ).text("Location of the directory containing all binaries.").required()

      opt[Int]('p', "partitions").action( (x, c) =>
        c.copy(partitions = x) ).text("Number of partitions used by executors.").required()

      opt[Int]("alphabet").action( (x, c) =>
        c.copy(alpbabet = x) ).text("Sets the alphabet used in motif iterator: 0: Exact, 1: Exact+N, 2: Exact+2fold+M, 3: All. Default is 2.")
      opt[Int]("degen").action( (x, c) =>
        c.copy(maxDegen = x) ).text("Sets the max number of degenerate characters.")
      opt[Int]("min_len").action( (x, c) =>
        c.copy(minMotifLen = x) ).text("Sets the minimum length of a motif.")
      opt[Int]("max_len").action( (x, c) =>
        c.copy(maxMotifLen = x) ).text("Sets the maximum length of a motif, this is not inclusive (i.e. length < maxLength).")
      opt[Int]("fam_cutoff").action( (x, c) =>
        c.copy(familyCountCutOff = x) ).text("Sets the number of families a motif needs to be part of to be valid. Default is 1.")
      opt[Int]("bg_model_count").action( (x, c) =>
        c.copy(backgroundModelCount = x) ).text("Sets the count of motifs in the background model. Default is 1000.")
      opt[Double]("conf_cutoff").action( (x, c) =>
        c.copy(confidenceScoreCutOff = x) ).text("Sets the cutoff for confidence scores. Default is 0.5.")

      opt[String]("bls_thresholds").action( (x, c) =>
        {
          val list = x.split(",").map(x => x.toFloat).toList
          c.copy(thresholdList = list)
        }).text("List of BLS threshold sepparated by a comma. Default is 0.15, 0.5, 0.6, 0.7, 0.9, 0.95. Currently a maximum of 8 thresholds is supported!")

      opt[String]("persist_level").action( (x, c) =>
          x match {
            case "mem" => c.copy(persistLevel = StorageLevel.MEMORY_ONLY)
            case "mem_ser" => c.copy(persistLevel = StorageLevel.MEMORY_ONLY_SER)
            case "mem_disk" => c.copy(persistLevel = StorageLevel.MEMORY_AND_DISK)
            case "mem_disk_ser" => c.copy(persistLevel = StorageLevel.MEMORY_AND_DISK_SER)
            case "disk" => c.copy(persistLevel = StorageLevel.DISK_ONLY)
            case _ => c.copy(persistLevel = StorageLevel.DISK_ONLY)
          }
         ).text("Sets the persist level for RDD's: mem, mem_ser, mem_disk, mem_disk_ser, disk [default]")


      opt[Unit]("AB").action( (_, c) =>
        c.copy(alignmentBased = true) ).text("Alignment based motif discovery.")

    }
    parser.parse(args, Config())
  }


  def runPipeline(config: Config) : Unit = {

    Timer.startTime()
    var tools = new Tools(config.bindir);
    var families = tools.readOrthologousFamilies(config.input, sc);

    info("family count: " + families.count);
    val motifs = tools.iterateMotifs(families, config.alignmentBased, config.alpbabet, config.maxDegen, config.minMotifLen, config.maxMotifLen, config.partitions, config.thresholdList);
    motifs.persist(config.persistLevel);
    info("motifs found: " + motifs.count());
    info(Timer.measureTime("motif count"))

    val groupedMotifs = tools.groupMotifsByGroup(motifs, config.maxMotifLen);
    info("groupedMotifs found: " + groupedMotifs.count());
    info(Timer.measureTime("grouped motifs count"))

    // val test = tools.processGroups(groupedMotifs, config.thresholdList, config.backgroundModelCount, config.familyCountCutOff, config.confidenceScoreCutOff)
    // test.collect().foreach(println)
    // deleteRecursively(config.output);
    // tools.stringRepresentation(motifs).saveAsTextFile(config.output);

    info(Timer.measureTotalTime("BlsSpeller"))
  }
}
