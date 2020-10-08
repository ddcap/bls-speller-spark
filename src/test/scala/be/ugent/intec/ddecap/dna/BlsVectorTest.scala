package be.ugent.intec.ddecap.dna

import org.scalatest.funsuite.AnyFunSuite

class BlsVectorTest extends AnyFunSuite {

  test("testAddByte") {
    val result = new BlsVector(Array(1, 2, 3))
    // 7 is 0b111 in bits
    result.addByte(7.toByte, 3)
    assert(result == new BlsVector(Array(2, 3, 4)))
  }

  test("testToString") {
    val result = new BlsVector(Array(1, 2, 3))
    assert(result.toString() == "1\t2\t3")
  }

  test("testAddVector") {
    val first = new BlsVector(Array(1, 2, 3))
    val result = first.addVector(new BlsVector(Array(4, 5, 6)))
    assert(result == new BlsVector(Array(5, 7, 9)))
  }

  test("testGetBlsVectorFromByte") {
    // 9 is 0b1001 in bits
    val result = BlsVectorFunctions.getBlsVectorFromByte(9.toByte, 4)
    assert(result == new BlsVector(Array(1, 0, 0, 1)))
  }
}
