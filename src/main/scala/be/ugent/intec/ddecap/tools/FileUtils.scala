package be.ugent.intec.ddecap.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{File, PrintWriter}
import org.apache.spark.SparkContext
import be.ugent.intec.ddecap.Logging
import org.apache.log4j.{Level, Logger}

object FileUtils {
  val logger = Logger.getLogger("be.ugent.intec.ddecap.tools.FileUtils");
  def downloadFile(conf: Configuration, filename: String, localDirectory: String) {
    val fs = FileSystem.get(conf);
    fs.copyToLocalFile(false, new Path(filename), new Path(localDirectory ), true);
  }

  def downloadDirectory(conf: Configuration, directory: String, localDirectory: String) {
    val fs = FileSystem.get(conf);
    val localDir = new Path(localDirectory)
    val statuses = fs.listStatus(new Path(directory));
    for(status <- statuses){
      logger.debug(status.getPath().toString());
      fs.copyToLocalFile(false, status.getPath(), localDir, true);
   }
  }
  def deleteRecursively(f: File): Boolean = {
    if(!f.exists()) return true // for if the folder doesn't exist!
    if (f.isDirectory) f.listFiles match {
      case null =>
      case xs   => xs foreach deleteRecursively
    }
    f.delete()
  }

  def deleteRecursively(path: String): Boolean = {
    val f: File = new File(path)
    deleteRecursively(f)
  }

  def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new PrintWriter(f)
    try { op(p) }
    finally { p.close() }
  }
  def printArrayToFile[T](array: Array[T], outputfile: String) = {
    printToFile(new File(outputfile)) {
      p => array.foreach(p.println)
    }
  }
  def readDict(sc: SparkContext, path: String): Map[String, Int] = {
    sc.textFile(path).filter(x => x.startsWith("@SQ"))
                     .zipWithIndex
                     .map(x => (x._1.split("\t")(1).split(":")(1), x._2.toInt)  )
                     .collect.toMap
  }

}
