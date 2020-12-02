package be.ugent.intec.ddecap.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import be.ugent.intec.ddecap.Logging

class BlsKryoSerializer extends KryoRegistrator with Logging {
  def registerByName(kryo: Kryo, name: String) {
   try {
     kryo.register(Class.forName(name))
   } catch {
     case cnfe: java.lang.ClassNotFoundException => {
       debug("Could not register class %s by name".format(name))
     }
   }
 }

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Array[Byte]]);
    kryo.register(classOf[be.ugent.intec.ddecap.dna.BlsVector]);
    kryo.register(classOf[scala.collection.mutable.ListBuffer[(List[Byte], be.ugent.intec.ddecap.dna.BlsVector)]]);
    registerByName(kryo, "scala.reflect.ClassTag$GenericClassTag")
    // kryo.register(classOf[scala.reflect.ClassTag$GenericClassTag]);
    // kryo.register(classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage]);
    // for sparkmeasure:
    // kryo.register(classOf[scala.reflect.ClassTag$$anon$1]);
    // kryo.register(classOf[Array[org.apache.spark.sql.catalyst.InternalRow]]);
    // kryo.register(classOf[org.apache.spark.sql.catalyst.InternalRow]);
    // kryo.register(classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult]);
    // kryo.register(classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary]);
    // kryo.register(classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats]);
  }
}
