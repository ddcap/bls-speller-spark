package be.ugent.intec.ddecap.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class BlsKryoSerializer extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Array[Byte]]);
    kryo.register(classOf[be.ugent.intec.ddecap.dna.BlsVector]);
    kryo.register(classOf[scala.collection.mutable.ListBuffer[(List[Byte], be.ugent.intec.ddecap.dna.BlsVector)]]);
    kryo.register(classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage]);
  }
}
