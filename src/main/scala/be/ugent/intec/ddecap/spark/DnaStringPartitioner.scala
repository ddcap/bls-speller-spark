package be.ugent.intec.ddecap.spark

import org.apache.spark.Partitioner
import be.ugent.intec.ddecap.Logging
import scala.collection.mutable.WrappedArray
import be.ugent.intec.ddecap.dna.ImmutableDnaPair
import scala.util.hashing.MurmurHash3

class DnaStringPartitioner(val maxNumPart: Int) extends Partitioner with be.ugent.intec.ddecap.Logging {
  val longBytes = 8;
  val bytearray: Array[Byte] = Array.fill(longBytes)(0)

  private def LongToByteArray(l: Long) {
      for (i <- 0 until longBytes) {
        bytearray(i) = (l >> (i*8) & 0xff).toByte
      }
  }
  private def LongHash(l: Long) : Int = {
    LongToByteArray(l)
    MurmurHash3.bytesHash(bytearray)
  }

  override def getPartition(key: Any): Int = {
    key match {
      case (l: Long) => Math.abs(LongHash(l) % maxNumPart)
      case (motifPair: ImmutableDnaPair) => Math.abs(LongHash(motifPair.group) % maxNumPart)
      case _ => {error("failed key is: " + key); throw new ClassCastException("failed key is: " + key)}
    }
  }
  override def numPartitions: Int = maxNumPart
}
