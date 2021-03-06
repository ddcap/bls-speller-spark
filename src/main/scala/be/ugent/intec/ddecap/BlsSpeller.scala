package be.ugent.intec.ddecap

import java.nio.file.{Files, Paths}

import be.ugent.intec.ddecap.BlsSpeller.LoggingMode.{LoggingMode, NO_LOGGING, SPARK_MEASURE}
import be.ugent.intec.ddecap.dna.BinaryDnaStringFunctions._
import be.ugent.intec.ddecap.dna.LongEncodedDna._
import be.ugent.intec.ddecap.rdd.RDDFunctions._
import be.ugent.intec.ddecap.tools.FileUtils.deleteRecursively
import be.ugent.intec.ddecap.tools.Tools
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
// import be.ugent.intec.ddecap.dna.BlsVector

object BlsSpeller extends Logging {
  object LoggingMode extends Enumeration {
    type LoggingMode = Value
    val NO_LOGGING, SPARK_MEASURE = Value
  }

  case class Config(
      mode: String = "getMotifs",
      input: String = "",
      motifs: String = "",
      fasta: String = "",
      partitions: Int = 8,
      alignmentBased: Boolean = false,
      bindir: String = "",
      output: String = "",
      maxDegen: Int = 4,
      minMotifLen: Int = 8,
      maxMotifLen: Int = 9,
      minBlsScore: Double = 0.0,
      alphabet: Int = 2, // 0: exact, 1: exact+N, 2: exact+2fold+M, 3: All
      familyCountCutOff: Int = 1,
      onlyiterate: Boolean = false,
      mapSideCombine: Boolean = false,
      useOldIterator: Boolean = false,
      similarityScore: Int = -1,
      emitRandomLowConfidenceScoreMotifs: Int = 0,
      numPartitionsForSecondStepFactor : Int = 4,
      minPartitionsForSecondStep: Int = 4,
      backgroundModelCount: Int = -1,
      confidenceScoreCutOff: Double = 0.5,
      thresholdList: List[Float] = List(0.15f, 0.5f, 0.6f, 0.7f, 0.9f, 0.95f),
      persistLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
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
              .set("spark.kryo.registrationRequired", "true")
              .set("spark.executor.processTreeMetrics.enabled", "true");
        val spark = SparkSession.builder
                                .appName("BLS Speller")
                                .config(conf)
                                .getOrCreate()
        sc = spark.sparkContext

        info("Apache Spark verion is " + sc.version)
        info("serializer is " + sc.getConf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer"))

//        deleteRecursively(config.output)
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
    val parser = new scopt.OptionParser[Config]("bls-speller") {
      head("BLS Speller", "0.2")

      opt[String]('i', "input").action( (x, c) =>
        c.copy(input = x) ).text("Location of the input files.").required()

      opt[String]('o', "output").action( (x, c) =>
        c.copy(output = x)).text("The output directory (spark output) or VCF output file (merged into a single file).").required()

      opt[String]('b', "bindir").action( (x, c) =>
        c.copy(bindir = x) ).text("Location of the directory containing all binaries.").required()

      opt[Int]('p', "partitions").action( (x, c) =>
        c.copy(partitions = x) ).text("Number of partitions used by executors.").required()

      opt[Int]("degen").action( (x, c) =>
        c.copy(maxDegen = x) ).text("Sets the max number of degenerate characters.").required()

      opt[Int]("fam_cutoff").action( (x, c) =>
        c.copy(familyCountCutOff = x) ).text("Sets the number of families a motif needs to be part of to be valid. Default is 1.")
      opt[Double]("conf_cutoff").action( (x, c) =>
        c.copy(confidenceScoreCutOff = x) ).text("Sets the cutoff for confidence scores. Default is 0.5.")

      opt[String]("persist_level").action( (x, c) =>
          x match {
            case "mem_disk" => c.copy(persistLevel = StorageLevel.MEMORY_AND_DISK)
            case "mem_disk_ser" => c.copy(persistLevel = StorageLevel.MEMORY_AND_DISK_SER)
            case "disk" => c.copy(persistLevel = StorageLevel.DISK_ONLY)
            case _ => c.copy(persistLevel = StorageLevel.MEMORY_AND_DISK)
          }
         ).text("Sets the persist level for RDD's: mem_disk, mem_disk_ser [default], disk")

      opt[String]("logging_mode").action( (x, c) =>
        x match {
          case "no_logging" => c.copy(loggingMode = LoggingMode.NO_LOGGING)
          case "spark_measure" => c.copy(loggingMode = LoggingMode.SPARK_MEASURE)
          case _ => c.copy(loggingMode = LoggingMode.NO_LOGGING)
        }
      ).text("Sets the logging mode: no_logging, spark_measure")


      opt[Unit]("AB").action( (_, c) =>
        c.copy(alignmentBased = true) ).text("Alignment based motif discovery.")

      // opt[Unit]("onlyiterate").action( (_, c) =>
        // c.copy(onlyiterate = true) ).text("Only iterate motifs, without processing.")

      opt[String]("bls_thresholds").action( (x, c) =>
      {
        val list = x.split(",").map(x => x.toFloat).toList
        c.copy(thresholdList = list)
      }).text("List of BLS threshold sepparated by a comma (example: 0.15,0.5,0.6,0.7,0.9,0.95).").required()

      // command to find motifs in ortho groups
        cmd("getMotifs").action( (_, c) => c.copy(mode = "getMotifs") ).
          text("get motifs of interest.").
          children(
            opt[Int]("alphabet").action( (x, c) =>
              c.copy(alphabet = x) ).text("Sets the alphabet used in motif iterator: 0: Exact, 1: Exact+N, 2: Exact+2fold+M, 3: All. Default is 2."),
            opt[Unit]("mapside_combine").action( (_, c) =>
              c.copy(mapSideCombine = true) ).text("Disables the map side combine."),
            opt[Unit]("old_iterator").action( (_, c) =>
              c.copy(useOldIterator = true) ).text("Use old way of iterating/merging. Uses a combinByKey with a Hashmap to collect motifs rather than using the Spark framework for this."),
            opt[Int]("min_len").action( (x, c) =>
              c.copy(minMotifLen = x, maxMotifLen = x + 1) ).text("Sets the minimum length of a motif.").required(),
            opt[Int]("bg_model_count").action( (x, c) =>
              c.copy(backgroundModelCount = x) ).text("Sets the count of motifs in the background model. Default is -1: count is based on the number of permutations of the group."),
            opt[Int]("similarity_score").action( (x, c) =>
              c.copy(similarityScore = x) ).text("Uses a similarity score to find the most dissimilar motifs in the background model. 0: Binary diff, 1: Hamming distance, 2: Levenshtein distance, Default is -1 [disabled]."),
            opt[Int]("emitrandommotifs").action( (x, c) =>
              c.copy(emitRandomLowConfidenceScoreMotifs = x) ).text("Emit random motifs below the c score threshold, one in every [int] motifs. Should be a high number, 100 000 or more. ")
          )

        cmd("locateMotifs").action( (_, c) => c.copy(mode = "locateMotifs") ).
          text("locates the found motifs in the given Ortho Groups.").
          children(
            opt[String]('m', "motifs").action( (x, c) =>
              c.copy(motifs = x) ).text("Folder with the motifs found by the first step of BLS Speller.").required(),
            opt[String]("fasta").action( (x, c) =>
              c.copy(fasta = x) ).text("Provide a fasta file with genes and location in the genome. The output will now only be locations in this genome, with the motifs."),

          // currently not implemented in the c++ score! for merged counts...
            opt[Int]("max_len").action( (x, c) =>
              c.copy(maxMotifLen = x) ).text("Sets the maximum length of a motif, this is not inclusive (i.e. length < maxLength).").required(),

            opt[Double]("min_bls").action( (x, c) =>
              c.copy(minBlsScore = x) ).text("Min bls score, emit motif locations only with a bls score of at least this value. Default is 0, i.e. no filtering. ")



          )
    }
    parser.parse(args, Config())
  }

  def toBinary(i: Int, digits: Int = 8) = "0000000" + i.toBinaryString takeRight digits

  def runPipeline(config: Config) : Unit = {
    Timer.startTime()
    var tools = new Tools(config.bindir);
    // info("setting # partitions to " + config.partitions)
    var families = tools.readOrthologousFamilies(config.input, config.partitions, sc);
    // families.persist(StorageLevel.DISK_ONLY)
    // val familiescount : Long = families.count
    // info("family count: " + familiescount );

    if(config.mode == "getMotifs") {
      val secondStepPartitions = Math.max(config.minPartitionsForSecondStep, config.numPartitionsForSecondStepFactor* config.partitions)
      var output = if(config.useOldIterator)
        {
          val motifs = tools.iterateMotifsOld(families, config.mode, config.alignmentBased, config.alphabet, config.maxDegen, config.minMotifLen, config.maxMotifLen, config.thresholdList);
          motifs.persist(config.persistLevel)
          val groupedMotifs = groupMotifsByGroup(motifs, config.thresholdList, secondStepPartitions);
          processHashMapGroups(groupedMotifs, config.thresholdList, config.backgroundModelCount, config.similarityScore, config.familyCountCutOff, config.confidenceScoreCutOff, config.emitRandomLowConfidenceScoreMotifs)
        } else if(config.mapSideCombine)
        {
          // combinebykey
          val motifs = tools.iterateMotifsAndMerge(families, config.mode, config.alignmentBased, config.alphabet, config.maxDegen, config.minMotifLen, config.maxMotifLen, config.thresholdList);
          motifs.persist(config.persistLevel)
          val motifsWithBlsCounts = countAndCollectdMotifs(motifs, secondStepPartitions); // Math.max(config.minimumPartitionsForSecondStep, config.partitions));
          processHashMapGroups(motifsWithBlsCounts, config.thresholdList, config.backgroundModelCount, config.similarityScore, config.familyCountCutOff, config.confidenceScoreCutOff, config.emitRandomLowConfidenceScoreMotifs)

          // reducebykey + combinebykey --> this is very slow! but maybe a bit more memory efficient, maybe...
          // val motifs = tools.iterateMotifPairsAndMerge(families, config.mode, config.alignmentBased, config.alphabet, config.maxDegen, config.minMotifLen, config.maxMotifLen, config.thresholdList);
          // motifs.persist(config.persistLevel)
          // val motifsWithBlsCounts = countBlsMergedMotifs(motifs, Math.max(config.minimumPartitionsForSecondStep, config.partitions));
          // val groupedMotifs = groupMotifsWithBlsCount(motifsWithBlsCounts, Math.max(config.minimumPartitionsForSecondStep, config.partitions));
          // processGroups(groupedMotifs, config.thresholdList, config.backgroundModelCount, config.similarityScore, config.familyCountCutOff, config.confidenceScoreCutOff)
        } else {
          val motifs = tools.iterateMotifs(families, config.mode, config.alignmentBased, config.alphabet, config.maxDegen, config.minMotifLen, config.maxMotifLen, config.thresholdList);
          motifs.persist(config.persistLevel)
          val motifsWithBlsCounts = countBls(motifs, config.thresholdList, secondStepPartitions);
          val groupedMotifs = groupMotifsWithBlsCount(motifsWithBlsCounts, secondStepPartitions);
          processGroups(groupedMotifs, config.thresholdList, config.backgroundModelCount, config.similarityScore, config.familyCountCutOff, config.confidenceScoreCutOff, config.emitRandomLowConfidenceScoreMotifs)
        }


      deleteRecursively(config.output)
      // if(config.onlyiterate)
        // motifsWithBlsCounts.map(x => LongToDnaString(x._1) + "\t" + LongToDnaString(x._2._1, config.maxMotifLen - 1) + "\t" + x._2._2).saveAsTextFile(config.output);
        // motifs.map(x => (x._1.map(b => toBinary(b, 8)).mkString(" ") + "\t" + x._2._1.map(b => toBinary(b, 8)).mkString(" ") + "\t" + toBinary(x._2._2, 8))).saveAsTextFile(config.output);
      // else
      // output.map(x => (LongToDnaString(x._4) + "\t" + LongToDnaString(x._1, getDnaLength(x._4)) + "\t" + x._2 + "\t" + x._3.mkString("\t") + "\t")).saveAsTextFile(config.output);
      output.map(x => (LongToDnaString(x._1, getDnaLength(x._4)) + "\t" + x._2 + "\t" + x._3.mkString("\t") + "\t")).saveAsTextFile(config.output);
      // families.unpersist()


  // for testing can write other rdd's to output:
      // deleteRecursively(config.output + "-motifs");
      // motifs.map(x => dnaToString(x._1) + "\t" + dnaWithoutLenToString(x._2._1, 8) + "\t" + x._2._2.toBinaryString).saveAsTextFile(config.output + "-motifs");
      // deleteRecursively(config.output + "-groupedmotifs");
      // groupedMotifs.map(x => dnaToString(x._1) + "\n" + x._2.map(y => (dnaWithoutLenToString(y._1, 8), y._2)).mkString("\t")).saveAsTextFile(config.output + "-groupedmotifs");

      info(Timer.measureTotalTime("BlsSpeller"))
    } else if(config.mode == "locateMotifs") {
      val motifs = tools.loadMotifs(config.motifs, config.partitions, sc, config.thresholdList, config.familyCountCutOff, config.confidenceScoreCutOff);
      val broadcastMotifs = sc.broadcast(motifs.sortBy(identity).collect)
      info("motif count after filter: " + broadcastMotifs.value.size);
      val motifLocations = tools.locateMotifs(families, config.mode, broadcastMotifs, config.alignmentBased, config.maxDegen, config.maxMotifLen, config.thresholdList, config.minBlsScore);

      deleteRecursively(config.output)
      if(config.fasta == "") {
        motifLocations.sortBy(_._1).map(x => x._1._1 + '\t' + x._1._2 + '\t' + x._2).saveAsTextFile(config.output);
      } else {
        // read the fasta file and get position in genome -> RDD[(gene_id, start_pos, end_pos)]
        val genomeLocations = tools.joinFastaAndLocations(config.fasta, config.partitions, sc, motifLocations, config.maxMotifLen)
        genomeLocations.sortBy(x=> x._1).map(x => x._1._1 + '\t' + x._1._2 + '\t' + (x._1._2 + config.maxMotifLen - 1)  + '\t' + x._2._1 + '\t' + x._2._2).saveAsTextFile(config.output);
      }
      info(Timer.measureTotalTime("BlsSpeller - locate Motifs"))
    } else {
      throw new Exception("invalid command")
    }
  }
}
