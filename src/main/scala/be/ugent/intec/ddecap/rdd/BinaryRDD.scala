package be.ugent.intec.ddecap.rdd

import org.apache.spark.rdd.{RDD}
import org.apache.spark.SparkContext
import java.io.{BufferedOutputStream, File, FileOutputStream}
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.io.{NullWritable, BytesWritable, LongWritable}
import org.apache.hadoop.conf.Configuration
import be.ugent.intec.ddecap.Logging
import org.apache.spark.rdd.RDD.rddToSequenceFileRDDFunctions

@SerialVersionUID(228L)
class BinaryRDDFunctions(rdd: RDD[Array[Byte]]) extends Serializable with Logging {

  def binaryToString() : RDD[String] = {
    rdd.map(x => new String(x.map(_.toChar)) )
  }

  def binSize() : Array[Long] = {
    rdd.map(x => x.size.toLong).mapPartitions(it => List(it.reduce(_+_)).iterator).collect
  }

  def saveAsBinaryFile(path: String): Long = {
      rdd.mapPartitionsWithIndex((idx, it) => {
        val filename = "%spart-%05d".format(path, idx)
        val yourFile = new File(filename);
        yourFile.getParentFile.mkdirs()
        val bos = new BufferedOutputStream(new FileOutputStream(filename))
        debug("saveAsBinFile() outputfile: " + filename)
        val towrite = it.reduce(_++_)
        debug("saveAsBinFile() outputfile: " + towrite.size)
        bos.write(towrite)
        debug("saveAsBinFile() write to: " + filename)
        bos.close
        debug("saveAsBinFile() closed: " + filename)
        it
      }).count // count makes it happen!
  }

  def saveBinaryWithIndex(path: String): Unit = {
    val codecOpt = Some(classOf[DefaultCodec])
    rdd.zipWithIndex.map(x => (new LongWritable(x._2), new BytesWritable(x._1)))
       .saveAsSequenceFile(path, codecOpt)
  }
}

object BinaryRDDFunctions {
  implicit def addBinaryRDDFunctions(rdd: RDD[Array[Byte]]) = new BinaryRDDFunctions(rdd)
}
