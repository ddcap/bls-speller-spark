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
    kryo.register(classOf[java.util.HashMap[Long, Int]]);
    kryo.register(classOf[be.ugent.intec.ddecap.dna.BlsVector]);
    kryo.register(classOf[scala.collection.mutable.ListBuffer[(List[Byte], be.ugent.intec.ddecap.dna.BlsVector)]]); //  Array[Int])]]);
    registerByName(kryo, "scala.reflect.ClassTag$GenericClassTag")
    kryo.register(classOf[be.ugent.intec.ddecap.dna.SimilarityScoreType]);
    kryo.register(classOf[be.ugent.intec.ddecap.dna.ImmutableDnaPair]);
    kryo.register(classOf[be.ugent.intec.ddecap.dna.ImmutableDnaWithBlsVectorByte]);
    kryo.register(classOf[be.ugent.intec.ddecap.dna.ImmutableDnaWithBlsVector]);
    kryo.register(scala.reflect.ClassTag(Class.forName("org.apache.spark.util.collection.CompactBuffer")).wrap.runtimeClass)
    registerByName(kryo, "org.apache.spark.util.collection.CompactBuffer[]")
    kryo.register(Class.forName("scala.reflect.ManifestFactory$AnyManifest", false, getClass().getClassLoader()));
  }
}
