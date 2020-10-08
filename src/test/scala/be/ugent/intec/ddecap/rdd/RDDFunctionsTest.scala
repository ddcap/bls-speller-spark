package be.ugent.intec.ddecap.rdd

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.funsuite.AnyFunSuite

// Examples for writing spark-testing-base tests: https://github.com/holdenk/spark-testing-base/wiki
class RDDFunctionsTest extends AnyFunSuite with SharedSparkContext with RDDComparisons {

  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }

  test("testGroupMotifsByGroup") {
    pending
  }

  test("testProcessGroups") {
    pending
  }
}
