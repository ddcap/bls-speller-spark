package be.ugent.intec.ddecap.dna
import collection.immutable.HashMap

final case class SimilarityScoreType(motifa: Long,  motifb: Long, len: Int);
final case class ImmutableDnaPair(motif: Long, group: Long);
final case class ImmutableDnaWithBlsVectorByte(motif: Long, blsbyte: Byte);
final case class ImmutableDnaWithBlsVector(motif: Long, vector: BlsVector);
// final case class ImmutableDnaWithBlsVector(motif: Long, vector: Array[Int]);
