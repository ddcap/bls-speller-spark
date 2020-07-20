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
      alignmentBased: Boolean = false,
      bindir: String = "",
      output: String = "")

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
      case None => throw new ClassCastException
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
    val motifs = tools.iterateMotifs(families, config.alignmentBased, families.count.toInt);
    motifs.persist(StorageLevel.MEMORY_ONLY);
    info("motifs found: " + motifs.count());
    info(Timer.measureTime("motif count"))

    // deleteRecursively(config.output);
    // tools.stringRepresentation(motifs).saveAsTextFile(config.output);

    info(Timer.measureTotalTime("BlsSpeller"))
  }
}
