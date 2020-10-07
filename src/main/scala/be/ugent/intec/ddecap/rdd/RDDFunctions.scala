package be.ugent.intec.ddecap.rdd

import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.hadoop.io.{NullWritable, BytesWritable, LongWritable}
import org.apache.hadoop.conf.Configuration
import collection.immutable.HashMap
import be.ugent.intec.ddecap.dna.BlsVector
import be.ugent.intec.ddecap.dna.BlsVectorFunctions._
import be.ugent.intec.ddecap.dna.BinaryDnaStringFunctions._
import be.ugent.intec.ddecap.spark.DnaStringPartitioner
import scala.collection.mutable.ListBuffer

object RDDFunctions {
  val logger = Logger.getLogger(getClass().getName());
  type ContentWithMotifAndBlsHashmap = (Array[Byte], HashMap[Array[Byte], BlsVector])

    def groupMotifsByGroup(input: RDD[(Seq[Byte], (Seq[Byte], Byte))], thresholdList: List[Float], partitions: Int) : RDD[(Seq[Byte], HashMap[Seq[Byte], BlsVector])] = {
      // per content group, we collect all motifs, per motif in this group we combine these together by adding the bls vector
      input.combineByKey(
          (p:(Seq[Byte], Byte)) => { // convert input value to output value
            HashMap((p._1 -> getBlsVectorFromByte(p._2, thresholdList.size)))
          },
          (map:HashMap[Seq[Byte], BlsVector], p:(Seq[Byte], Byte)) => { // merge an input value with an already existing output value
            if(map.contains(p._1))
              map.merged(HashMap((p._1 -> getBlsVectorFromByte(p._2, thresholdList.size))))({
                case ((k,v1),(_,v2)) => (k,v1.addVector(v2))
              })
            else {
              map + (p._1 -> getBlsVectorFromByte(p._2, thresholdList.size))
            }
          },
          (map1:HashMap[Seq[Byte], BlsVector], map2:HashMap[Seq[Byte], BlsVector]) => { // merge two existing output values together
            map1.merged(map2)({
              case ((k,v1:BlsVector),(_,v2:BlsVector)) => (k,v1.addVector(v2))
            })
          },
          new DnaStringPartitioner(partitions), true) // mapsidecombine cannot be true with array as key....
    }

    def processGroups(input: RDD[(Seq[Byte], HashMap[Seq[Byte], BlsVector])],
        thresholdList: List[Float],
        backgroundModelCount: Int, familyCountCutOff: Int, confidenceScoreCutOff: Double) : RDD[(Seq[Byte], Byte, BlsVector, List[Float])] = {
      input.flatMap(x => { // x is an iterator over the motifs+blsvector in this group
        val key = x._1
        val data = x._2
        val bgmodel = generateBackgroundModel(key, backgroundModelCount)
        val median : BlsVector = getMedianPerThreshold(data, bgmodel, thresholdList.size)
        // info("median: " + median)
        val retlist = ListBuffer[(Seq[Byte], Byte, BlsVector, List[Float])]()
        for( d <- data) { // for each motif calculate every F(Ti) and corresponding C(Ti)
          val conf_score_vector = Array.fill(thresholdList.size)(0.0f)
          var thresholds_passed = false;
          for(t <- 0 until thresholdList.size) {  // for every Threshold Ti:
            val family_count_t = d._2.getThresholdCount(t) // F(Ti)
            val family_count_bg_t = median.getThresholdCount(t).toFloat
            conf_score_vector(t) = if(family_count_t > family_count_bg_t) 1.0f - family_count_bg_t / family_count_t else 0.0f; // C(Ti)
            if(family_count_t >= familyCountCutOff && conf_score_vector(t) >= confidenceScoreCutOff) {
              // emit motif if any Ti has a valid cutoff
              thresholds_passed = true;
            }
          }
          if(thresholds_passed) retlist += ((d._1, x._1(0), d._2, conf_score_vector.toList))
        }
        retlist
        })
    }

}
