package be.ugent.intec.ddecap.spark

import org.apache.spark.Partitioner
import be.ugent.intec.ddecap.Logging
import scala.collection.mutable.WrappedArray
import be.ugent.intec.ddecap.dna.ImmutableDnaPair

class DnaStringPartitioner(val maxNumPart: Int) extends Partitioner with be.ugent.intec.ddecap.Logging {
  override def getPartition(key: Any): Int = {
    key match {
      case (l: Long) => Math.abs(l.hashCode % maxNumPart)
      case (motifPair: ImmutableDnaPair) => Math.abs(motifPair.group.hashCode % maxNumPart)
      case _ => {error("failed key is: " + key); throw new ClassCastException("failed key is: " + key)}
    }
  }
  override def numPartitions: Int = maxNumPart
}
