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
  val logger = Logger.getLogger("be.ugent.intec.ddecap.rdd.RDDFunctions");
  type ImmutableDna = Long

    def groupMotifsByGroup(input: RDD[(ImmutableDna, (ImmutableDna, Byte))], thresholdList: List[Float], partitions: Int) : RDD[(ImmutableDna, HashMap[ImmutableDna, BlsVector])] = {
      // per content group, we collect all motifs, per motif in this group we combine these together by adding the bls vector
      input.combineByKey(
          (p:(ImmutableDna, Byte)) => { // convert input value to output value
            HashMap((p._1 -> getBlsVectorFromByte(p._2, thresholdList.size)))
          },
          (map:HashMap[ImmutableDna, BlsVector], p:(ImmutableDna, Byte)) => { // merge an input value with an already existing output value
            if(map.contains(p._1))
              map.merged(HashMap((p._1 -> getBlsVectorFromByte(p._2, thresholdList.size))))({
                case ((k,v1),(_,v2)) => (k,v1.addVector(v2))
              })
            else {
              map + (p._1 -> getBlsVectorFromByte(p._2, thresholdList.size))
            }
          },
          (map1:HashMap[ImmutableDna, BlsVector], map2:HashMap[ImmutableDna, BlsVector]) => { // merge two existing output values together
            map1.merged(map2)({
              case ((k,v1:BlsVector),(_,v2:BlsVector)) => (k,v1.addVector(v2))
            })
          },
          new DnaStringPartitioner(partitions), true) // mapsidecombine cannot be true with array as key....
    }

    val emitRandomLowConfidenceScoreMotifs = 100000;
    def processGroups(input: RDD[(ImmutableDna, HashMap[ImmutableDna, BlsVector])],
        thresholdList: List[Float],
        backgroundModelCount: Int, similarityScore: Int,
        familyCountCutOff: Int, confidenceScoreCutOff: Double) :
        RDD[(ImmutableDna, BlsVector, List[Float], ImmutableDna)] = {
      input.flatMap(x => { // x is an iterator over the motifs+blsvector in this group
        val key = x._1
        val data = x._2
        val rnd = new scala.util.Random
        val bgmodel = generateBackgroundModel(key, backgroundModelCount, similarityScore)
        val median : BlsVector = getMedianPerThreshold(data, bgmodel, thresholdList.size)
        // logger.info(dnaToString(key) + " median: " + median)
        val retlist = ListBuffer[(ImmutableDna, BlsVector, List[Float], ImmutableDna)]()
        for( d <- data) { // for each motif calculate every F(Ti) and corresponding C(Ti)
          val conf_score_vector = Array.fill(thresholdList.size)(0.0f)
          var thresholds_passed = false;
          for(t <- 0 until thresholdList.size) {  // for every Threshold Ti:
            val family_count_t = d._2.getThresholdCount(t) // F(Ti)
            val family_count_bg_t = median.getThresholdCount(t).toFloat
            conf_score_vector(t) = if(family_count_t > family_count_bg_t) (family_count_t - family_count_bg_t) / family_count_t else 0.0f; // C(Ti)
            if(family_count_t >= familyCountCutOff && conf_score_vector(t) >= confidenceScoreCutOff) {
              // emit motif if any Ti has a valid cutoff
              thresholds_passed = true;
            } else {
              if (rnd.nextInt(emitRandomLowConfidenceScoreMotifs) == 0){
                // logger.info("emitting motif below c threshold " + confidenceScoreCutOff + " for tests.")
                thresholds_passed = true;
              }
            }
          }
          // logger.info(dnaWithoutLenToString(d._1, key(0)) + " " + d._2 + " " + conf_score_vector.toList)
          if(thresholds_passed) retlist += ((d._1, d._2, conf_score_vector.toList, key))
        }
        retlist
        })
    }

}
