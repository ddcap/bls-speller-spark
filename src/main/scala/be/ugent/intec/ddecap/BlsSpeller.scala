package be.ugent.intec.ddecap

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import be.ugent.intec.ddecap.bio.spark.rdd.SamRDDFunctions._
import be.ugent.intec.ddecap.bio.spark.rdd.BinaryRDDFunctions._
import be.ugent.intec.ddecap.bio.tools.{Bwa, Gatk, Elprep}
import be.ugent.intec.ddecap.bio.PrepUtils
import be.ugent.intec.ddecap.bio.Utils._
import be.ugent.intec.ddecap.bio.FileUtils._
import org.apache.spark.storage.StorageLevel
import java.io._
import scala.io.Source
import java.util.zip._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf;
import be.ugent.intec.ddecap.bio.prep.Preprocessor
import htsjdk.samtools.SAMRecord

object DnaPipeline extends Logging {
  case class Config(
      input: String = "",
      output: String = "")

  var sc: SparkContext = null

  def main(args: Array[String]) {

    val optionConfig = parseOptionsAndStart(args)
    optionConfig match {
      case Some(config) =>
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        var conf: SparkConf = null;
        if(config.useKryo) {
          conf = new SparkConf()
              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .set("spark.kryo.registrator","be.ugent.intec.ddecap.bio.spark.BioKryoSerializer")
              .set("spark.kryoserializer.buffer.mb","128")
              .set("spark.kryo.registrationRequired", "true");
        } else {
          conf = new SparkConf();
        }
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
    val parser = new scopt.OptionParser[Config]("dna-pipeline") {
      head("Halvade Spark DNA pipeline", "1.5")

      opt[String]('i', "input").action( (x, c) =>
        c.copy(input = x) ).text("Location of the input files.").required()

      opt[String]('o', "output").action( (x, c) =>
        c.copy(output = x).copy(mergeVcf = (if (x.endsWith(".vcf")) true else false) ) ).text("The output directory (spark output) or VCF output file (merged into a single file).").required()

    }
    parser.parse(args, Config())
  }


  def runPipeline(config: Config) : Unit = {

    startTime()

    info(measureTime("BlsSpeller"))
  }
}
