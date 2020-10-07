package be.ugent.intec.ddecap.spark

import org.apache.spark.Partitioner
import be.ugent.intec.ddecap.Logging
import scala.collection.mutable.WrappedArray

class DnaStringPartitioner(val maxNumPart: Int) extends Partitioner with Logging {
  override def getPartition(key: Any): Int = {
    key match {
      case (dnastring: WrappedArray[Byte]) => { // is Seq[Byte]
        // should be exactly one element always!
        var hashCode: Int  = dnastring(1) // cause first bit is always length! and can be same...
        for(i <- 2 until Math.max(5, dnastring.size)) {
          hashCode |= dnastring(i) << (8*(i-1))
        }
        for(i <- 5 until dnastring.size) {
          hashCode ^= dnastring(i) << (8*((i-1)%4))
        }
        // info("hash: " + hashCode)
        Math.abs(hashCode % maxNumPart)
      }
      case (dnastring: Array[Byte]) => { 
        // should be exactly one element always!
        var hashCode: Int  = dnastring(1) // cause first bit is always length! and can be same...
        for(i <- 2 until Math.max(5, dnastring.size)) {
          hashCode |= dnastring(i) << (8*(i-1))
        }
        for(i <- 5 until dnastring.size) {
          hashCode ^= dnastring(i) << (8*((i-1)%4))
        }
        // info("hash: " + hashCode)
        Math.abs(hashCode % maxNumPart)
      }
      case _ => {error("failed key is: " + key); throw new ClassCastException("failed key is: " + key)}
    }
  }
  override def numPartitions: Int = maxNumPart
}
