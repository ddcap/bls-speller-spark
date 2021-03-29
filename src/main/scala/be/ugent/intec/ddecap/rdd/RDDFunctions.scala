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
import be.ugent.intec.ddecap.dna.LongEncodedDna._
import be.ugent.intec.ddecap.dna.{ImmutableDnaPair, ImmutableDnaWithBlsVectorByte, ImmutableDnaWithBlsVector}

object RDDFunctions {
  val logger = Logger.getLogger("be.ugent.intec.ddecap.rdd.RDDFunctions");
  type ImmutableDna = Long
  // type BlsVector = Array[Int];

    def groupMotifsByGroup(input: RDD[(ImmutableDna, ImmutableDnaWithBlsVectorByte)], thresholdList: List[Float], partitions: Int, mapSideCombine: Boolean = true) : RDD[(ImmutableDna, HashMap[ImmutableDna, BlsVector])] = {
      // per content group, we collect all motifs, per motif in this group we combine these together by adding the bls vector
      input.combineByKey(
          (p: ImmutableDnaWithBlsVectorByte) => { // convert input value to output value
            HashMap((p.motif -> getBlsVectorFromByte(p.blsbyte, thresholdList.size)))
          },
          (map:HashMap[ImmutableDna, BlsVector], p:ImmutableDnaWithBlsVectorByte) => { // merge an input value with an already existing output value
            if(map.contains(p.motif)) {
              map(p.motif).addByte(p.blsbyte)
              // for (i <- 0 until p.blsbyte) {
                // map(p.motif)(i) += 1
              // }
              map
              // map.merged(HashMap((p.motif -> getBlsVectorFromByte(p.blsbyte, thresholdList.size))))({
              //   // case ((k,v1),(_,v2)) => (k, v1.addVector(v2))
              //   case ((k,v1),(_,v2)) => {
              //     for (i <- 0 until v1.length) {
              //        v1(i) += v2(i)
              //      }
              //     (k, v1)
              //   }
              // })
            } else {
              map + (p.motif -> getBlsVectorFromByte(p.blsbyte, thresholdList.size))
            }
          },
          (map1:HashMap[ImmutableDna, BlsVector], map2:HashMap[ImmutableDna, BlsVector]) => { // merge two existing output values together
            map1.merged(map2)({
              case ((k,v1:BlsVector),(_,v2:BlsVector)) => (k,v1.addVector(v2))
              // case ((k,v1:BlsVector),(_,v2:BlsVector)) => {
              //   for (i <- 0 until v1.length) {
              //      v1(i) += v2(i)
              //    }
              //   (k, v1)
              // }
            })
          },
          new DnaStringPartitioner(partitions), mapSideCombine) // mapsidecombine cannot be true with array as key....
    }
    def countAndCollectdMotifs(input: RDD[(ImmutableDna, ImmutableDnaWithBlsVector)], partitions: Int) : RDD[(ImmutableDna, HashMap[ImmutableDna, BlsVector])] = {
      // per content group, we collect all motifs, per motif in this group we combine these together by adding the bls vector
      input.combineByKey(
          (p: ImmutableDnaWithBlsVector) => { // convert input value to output value
            HashMap((p.motif -> p.vector))
          },
          (map:HashMap[ImmutableDna, BlsVector], p:ImmutableDnaWithBlsVector) => { // merge an input value with an already existing output value
            if(map.contains(p.motif)) {
              map(p.motif).addVector(p.vector)
              map
            } else {
              map + (p.motif -> p.vector)
            }
          },
          (map1:HashMap[ImmutableDna, BlsVector], map2:HashMap[ImmutableDna, BlsVector]) => { // merge two existing output values together
            map1.merged(map2)({
              case ((k,v1:BlsVector),(_,v2:BlsVector)) => (k,v1.addVector(v2))
            })
          }, new DnaStringPartitioner(partitions), false)
    }

    def countBls(input: RDD[(ImmutableDnaPair, Byte)], thresholdList: List[Float], partitions: Int, mapSideCombine: Boolean = true) : RDD[(ImmutableDna, ImmutableDnaWithBlsVector)] = {
      input.combineByKey(
        (b: Byte) => {
          getBlsVectorFromByte(b, thresholdList.size)
        }, (count: BlsVector, b: Byte) => {
          count.addByte(b)
          // for (i <- 0 until b) {
          //    count(i) += 1
          //  }
          //  count
        }, (count1:BlsVector, count2:BlsVector) => {
          count1.addVector(count2)
          // for (i <- 0 until count1.length) {
          //    count1(i) += count2(i)
          //  }
          //  count1
        }, new DnaStringPartitioner(partitions), mapSideCombine).map(x => (x._1.group, ImmutableDnaWithBlsVector(x._1.motif, x._2)) )
    }

    def countBlsMergedMotifs(input: RDD[(ImmutableDnaPair, BlsVector)], partitions: Int) : RDD[(ImmutableDna, ImmutableDnaWithBlsVector)] = {
      input.combineByKey(
            (p: BlsVector) => {
              p
            }, (merge:BlsVector, p:BlsVector) => {
              merge.addVector(p)
            },
            (merge1:BlsVector, merge2:BlsVector) => {
              merge1.addVector(merge2)
            }, new DnaStringPartitioner(partitions), false)
          .map(x => (x._1.group, ImmutableDnaWithBlsVector(x._1.motif, x._2)) )
    }

    def groupMotifsWithBlsCount(input:  RDD[(ImmutableDna, ImmutableDnaWithBlsVector)], partitions: Int, mapSideCombine: Boolean = true) :  RDD[(ImmutableDna, List[ImmutableDnaWithBlsVector])] = {
      input.combineByKey(
        (a: ImmutableDnaWithBlsVector) => {
          List((a))
        }, (list: List[ImmutableDnaWithBlsVector], b: ImmutableDnaWithBlsVector) => {
          list ::: List((b))
        }, (list1: List[ImmutableDnaWithBlsVector], list2: List[ImmutableDnaWithBlsVector]) => {
          list1 ::: list2
        }, new DnaStringPartitioner(partitions), mapSideCombine)
    }


    def processGroups(input: RDD[(ImmutableDna, List[ImmutableDnaWithBlsVector])],
        thresholdList: List[Float],
        backgroundModelCount: Int, similarityScore: Int,
        familyCountCutOff: Int, confidenceScoreCutOff: Double,
        emitRandomLowConfidenceScoreMotifs: Int = 0) :
        RDD[(ImmutableDna, BlsVector, List[Float], ImmutableDna)] = {
      input.flatMap(x => { // x is group + motifs+blsvector list
        val key = x._1 // motif group (based on character content)
        val data = x._2 // list of motifs with same character content with corresponding blsvector
        val groupIsItsOwnRC = isGroupItsOwnRc(key);
        val len = getDnaLength(key)
        // logger.info(data.size + " motifs in this group")
        val rnd = new scala.util.Random
        // val bgmodel = generateBackgroundModel(key, backgroundModelCount, similarityScore)
        // val median : BlsVector = getMedianPerThreshold(data, bgmodel, thresholdList.size)
        val median : BlsVector = getMedianPerThreshold(key, data, thresholdList.size)
        val retlist = ListBuffer[(ImmutableDna, BlsVector, List[Float], ImmutableDna)]()
        for( d <- data) { // for each motif calculate every F(Ti) and corresponding C(Ti)
          val conf_score_vector = Array.fill(thresholdList.size)(0.0f)
          var thresholds_passed = false;
          var emit_rnd = false;
          for(t <- 0 until thresholdList.size) {  // for every Threshold Ti:
            val family_count_t = d.vector.getThresholdCount(t) // F(Ti)
            val family_count_bg_t = median.getThresholdCount(t).toFloat
            conf_score_vector(t) = if(family_count_t > family_count_bg_t) (family_count_t - family_count_bg_t) / family_count_t else 0.0f; // C(Ti)
            if(family_count_t >= familyCountCutOff && conf_score_vector(t) >= confidenceScoreCutOff) {
              // emit motif if any Ti has a valid cutoff
              thresholds_passed = true;
            } else {
              if (emitRandomLowConfidenceScoreMotifs > 0 && rnd.nextInt(emitRandomLowConfidenceScoreMotifs) == 0){
                // logger.info("emitting motif below c threshold " + confidenceScoreCutOff + " for tests.")
                emit_rnd = true;
              }
            }
          }
          // logger.info(dnaWithoutLenToString(d._1, key(0)) + " " + d._2 + " " + conf_score_vector.toList)
          if(thresholds_passed) {
            if(groupIsItsOwnRC) { // avoid emitting motifs twice in this group
              if(isRepresentative(d.motif, len)) {
                retlist += ((d.motif, d.vector, conf_score_vector.toList, key))
              }
            } else {
              retlist += ((d.motif, d.vector, conf_score_vector.toList, key))
            }
          } else if (emit_rnd) {
            retlist += ((d.motif, d.vector, conf_score_vector.toList, key))
          }
        }
        // logger.info("[" + key + " filter] time (s): "+(System.nanoTime-starttime)/1.0e9)
        retlist
        })
    }

    def processHashMapGroups(input: RDD[(ImmutableDna, HashMap[ImmutableDna, BlsVector])],
        thresholdList: List[Float],
        backgroundModelCount: Int, similarityScore: Int,
        familyCountCutOff: Int, confidenceScoreCutOff: Double,
        emitRandomLowConfidenceScoreMotifs: Int = 0) :
        RDD[(ImmutableDna, BlsVector, List[Float], ImmutableDna)] = {
      input.flatMap(x => { // x is an iterator over the motifs+blsvector in this group
        val key = x._1
        val groupIsItsOwnRC = isGroupItsOwnRc(key);
        val len = getDnaLength(key)
        val data = x._2
        // logger.info(data.size + " motifs in this group")
        val rnd = new scala.util.Random
        // var starttime = System.nanoTime
        // val bgmodel = generateBackgroundModel(key, backgroundModelCount, similarityScore)
        // val median : BlsVector = oldGetMedianPerThreshold(data, bgmodel, thresholdList.size)
        val median : BlsVector = getMedianPerThreshold(key, data, thresholdList.size)
        // logger.info("[" + key + " bg model] time (s): "+(System.nanoTime-starttime)/1.0e9)
        // logger.info(dnaToString(key) + " median: " + median)
        // starttime = System.nanoTime
        val retlist = ListBuffer[(ImmutableDna, BlsVector, List[Float], ImmutableDna)]()
        for( d <- data) { // for each motif calculate every F(Ti) and corresponding C(Ti)
          val conf_score_vector = Array.fill(thresholdList.size)(0.0f)
          var thresholds_passed = false;
          var emit_rnd = false;
          for(t <- 0 until thresholdList.size) {  // for every Threshold Ti:
            val family_count_t = d._2.getThresholdCount(t) // F(Ti)
            val family_count_bg_t = median.getThresholdCount(t).toFloat
            conf_score_vector(t) = if(family_count_t > family_count_bg_t) (family_count_t - family_count_bg_t) / family_count_t else 0.0f; // C(Ti)
            if(family_count_t >= familyCountCutOff && conf_score_vector(t) >= confidenceScoreCutOff) {
              // emit motif if any Ti has a valid cutoff
              thresholds_passed = true;
            } else {
              if (emitRandomLowConfidenceScoreMotifs > 0 && rnd.nextInt(emitRandomLowConfidenceScoreMotifs) == 0){
                // logger.info("emitting motif below c threshold " + confidenceScoreCutOff + " for tests.")
                emit_rnd = true;
              }
            }
          }
          // logger.info(dnaWithoutLenToString(d._1, key(0)) + " " + d._2 + " " + conf_score_vector.toList)
          if(thresholds_passed) {
            if(groupIsItsOwnRC) { // avoid emitting motifs twice in this group
              if(isRepresentative(d._1, len)) {
                retlist += ((d._1, d._2, conf_score_vector.toList, key))
              }
            } else {
              retlist += ((d._1, d._2, conf_score_vector.toList, key))
            }
          } else if (emit_rnd) {
            retlist += ((d._1, d._2, conf_score_vector.toList, key))
          }
        }
        // logger.info("[" + key + " filter] time (s): "+(System.nanoTime-starttime)/1.0e9)
        retlist
        })
    }
}
