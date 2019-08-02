/*
package com.cway.spark.core

import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory

class MyAccumulator extends AccumulatorV2[String, String] {

  private val log = LoggerFactory.getLogger("MyAccumulatorV2")
  var result = "user0=0|user1=0|user2=0|user3=0"

  override def isZero: Boolean = {
    result == "user0=0|user1=0|user2=0|user3=0"
  }

  override def copy(): AccumulatorV2[String, String] = {
    val myAccumulator = new MyAccumulator()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = "user0=0|user1=0|user2=0|user3=0"
  }

  override def add(v: String): Unit = {
    val v1 = result
    val v2 = v
    //    log.warn("v1 : " + v1 + " v2 : " + v2)
    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      var newResult = ""
      // 从v1中，提取v2对应的值，并累加
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        val newValue = oldValue.toInt + 1
        newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
      result = newResult
    }
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    case map: MyAccumulator =>
      result = other.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = {
    result
  }

}
*/
