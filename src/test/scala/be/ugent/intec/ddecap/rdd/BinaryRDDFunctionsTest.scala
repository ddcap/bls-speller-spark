package be.ugent.intec.ddecap.rdd

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

// Examples for writing spark-testing-base tests: https://github.com/holdenk/spark-testing-base/wiki
class BinaryRDDFunctionsTest extends AnyFunSuite with SharedSparkContext with RDDComparisons {

  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }

  test("testBinaryToString") {
    val expectedRDD : RDD[String] = sc.parallelize(Seq("ABC", "DEF"))
    val resultRDD = new BinaryRDDFunctions(sc.parallelize(Seq(Array(65, 66, 67), Array(68, 69, 70)))).binaryToString()
    assert(None === compareRDD(expectedRDD, resultRDD))
  }

  test("testBinSizePerPartition") {
    val expectedRDD : Array[Long] = Array(0, 0, 0, 0, 0 ,3, 0, 0, 0, 0, 0, 3)
    val resultRDD = new BinaryRDDFunctions(sc.parallelize(Seq(Array(65, 66, 67), Array(68, 69, 70)))).binSizePerPartition()
    assert(resultRDD.deep == expectedRDD.deep)
  }

  test("testBinSize") {
    val expected : Long = 6
    val result = new BinaryRDDFunctions(sc.parallelize(Seq(Array(65, 66, 67), Array(68, 69, 70)))).binSize()
    assert(result == expected)
  }

}
