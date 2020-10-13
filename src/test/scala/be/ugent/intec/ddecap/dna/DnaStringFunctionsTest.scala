package be.ugent.intec.ddecap.dna

import be.ugent.intec.ddecap.dna.BinaryDnaStringFunctions.dnaToString
import org.scalatest.funsuite.AnyFunSuite

class DnaStringFunctionsTest extends AnyFunSuite {

  test("testSplitBinaryDataInMotifAndBlsVector") {
    pending
  }

  test("testDnaToString") {
    // TODO check validity of test
    val input: Array[Byte] = Array(1, 1)
    val result = dnaToString(input)
    assert(result == "A")
  }

  test("testGetGroupId") {
    pending
  }

}
