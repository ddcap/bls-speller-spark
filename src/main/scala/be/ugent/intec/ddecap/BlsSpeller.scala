package be.ugent.intec.ddecap

import be.ugent.intec.ddecap.BlsSpeller.LoggingMode.{LoggingMode, NO_LOGGING, SPARK_MEASURE}
import be.ugent.intec.ddecap.dna.BinaryDnaStringFunctions._
import be.ugent.intec.ddecap.dna.LongEncodedDna._
import be.ugent.intec.ddecap.rdd.RDDFunctions._
import be.ugent.intec.ddecap.tools.FileUtils._
import be.ugent.intec.ddecap.tools.Tools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object BlsSpeller extends Logging {
  object LoggingMode extends Enumeration {
    type LoggingMode = Value
    val NO_LOGGING, SPARK_MEASURE = Value
  }

  case class Config(
      input: String = "",
      partitions: Int = 8,
      alignmentBased: Boolean = false,
      bindir: String = "",
      output: String = "",
      maxDegen: Int = 4,
      minMotifLen: Int = 8,
      maxMotifLen: Int = 9,
      alphabet: Int = 2, // 0: exact, 1: exact+N, 2: exact+2fold+M, 3: All
      familyCountCutOff: Int = 1,
      onlyiterate: Boolean = false,
      backgroundModelCount: Int = 1000,
      confidenceScoreCutOff: Double = 0.5,
      thresholdList: List[Float] = List(0.15f, 0.5f, 0.6f, 0.7f, 0.9f, 0.95f),
      persistLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
      loggingMode: LoggingMode = LoggingMode.NO_LOGGING
    )

  var sc: SparkContext = null

  def main(args: Array[String]) {

    val optionConfig = parseOptionsAndStart(args)
    optionConfig match {
      case Some(config) =>
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        var conf: SparkConf = null;
        conf = new SparkConf()
              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .set("spark.kryo.registrator","be.ugent.intec.ddecap.spark.BlsKryoSerializer")
              .set("spark.kryoserializer.buffer.mb","128")
              .set("spark.kryo.registrationRequired", "true");
        val spark = SparkSession.builder
                                .appName("BLS Speller")
                                .config(conf)
                                .getOrCreate()
        sc = spark.sparkContext

        info("serializer is " + sc.getConf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer"))

        deleteRecursively(config.output)
        config.loggingMode match {
          case NO_LOGGING => runPipeline(config)
          case SPARK_MEASURE =>
            val stageMetrics = ch.cern.sparkmeasure.StageMetrics(spark)
            stageMetrics.begin()
            runPipeline(config)
            stageMetrics.end()
            stageMetrics.printReport()
            stageMetrics.printAccumulables()
            val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
            stageMetrics.saveData(df.orderBy("jobId", "stageId"), config.output + "/stageMetrics")
            val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
            stageMetrics.saveData(aggregatedDF, config.output + "/aggregateStageMetrics")
        }
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
        c.copy(alphabet = x) ).text("Sets the alphabet used in motif iterator: 0: Exact, 1: Exact+N, 2: Exact+2fold+M, 3: All. Default is 2.")
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
            case _ => c.copy(persistLevel = StorageLevel.MEMORY_AND_DISK)
          }
         ).text("Sets the persist level for RDD's: mem, mem_ser, mem_disk [default], mem_disk_ser, disk")

      opt[String]("logging_mode").action( (x, c) =>
        x match {
          case "no_logging" => c.copy(loggingMode = LoggingMode.NO_LOGGING)
          case "spark_measure" => c.copy(loggingMode = LoggingMode.SPARK_MEASURE)
          case _ => c.copy(loggingMode = LoggingMode.NO_LOGGING)
        }
      ).text("Sets the logging mode: no_logging, spark_measure")


      opt[Unit]("AB").action( (_, c) =>
        c.copy(alignmentBased = true) ).text("Alignment based motif discovery.")

      opt[Unit]("onlyiterate").action( (_, c) =>
        c.copy(onlyiterate = true) ).text("Only iterate motifs, without processing.")
    }
    parser.parse(args, Config())
  }

  def toBinary(i: Int, digits: Int = 8) = "0000000" + i.toBinaryString takeRight digits

  def runPipeline(config: Config) : Unit = {
    Timer.startTime()
    var tools = new Tools(config.bindir);
    var families = tools.readOrthologousFamilies(config.input, config.partitions, sc);

    info("family count: " + families.count);
    val motifs = tools.iterateMotifs(families, config.alignmentBased, config.alphabet, config.maxDegen, config.minMotifLen, config.maxMotifLen, config.thresholdList);
    val groupedMotifs = groupMotifsByGroup(motifs, config.thresholdList, config.partitions);
    val output = processGroups(groupedMotifs, config.thresholdList, config.backgroundModelCount, config.familyCountCutOff, config.confidenceScoreCutOff)

    // motifs.persist(config.persistLevel)
    // info("motifs count: " + motifs.count);
    // groupedMotifs.persist(config.persistLevel)
    // info("groupedMotifs count: " + groupedMotifs.count);
    deleteRecursively(config.output);
    if(config.onlyiterate)
      motifs.map(x => LongToDnaString(x._1) + "\t" + LongToDnaString(x._2._1, config.maxMotifLen - 1) + "\t" + toBinary(x._2._2)).saveAsTextFile(config.output);
      // motifs.map(x => (x._1.map(b => toBinary(b, 8)).mkString(" ") + "\t" + x._2._1.map(b => toBinary(b, 8)).mkString(" ") + "\t" + toBinary(x._2._2, 8))).saveAsTextFile(config.output);
    else
      output.map(x => (LongToDnaString(x._1, getDnaLength(x._4)) + "\t" + x._2 + "\t" + x._3.mkString("\t") + "\t")).saveAsTextFile(config.output);

// for testing can write other rdd's to output:
    // deleteRecursively(config.output + "-motifs");
    // motifs.map(x => dnaToString(x._1) + "\t" + dnaWithoutLenToString(x._2._1, 8) + "\t" + x._2._2.toBinaryString).saveAsTextFile(config.output + "-motifs");
    // deleteRecursively(config.output + "-groupedmotifs");
    // groupedMotifs.map(x => dnaToString(x._1) + "\n" + x._2.map(y => (dnaWithoutLenToString(y._1, 8), y._2)).mkString("\t")).saveAsTextFile(config.output + "-groupedmotifs");

    info(Timer.measureTotalTime("BlsSpeller"))
  }
}
