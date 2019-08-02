package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AddFAQ {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("AddFAQ")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    var count = 0.0
    //广播变量  全局的大常量
    /**
      * 累加器    全局的大变量
      * 累加类是在Driver端维护
      */

    val accumulator = sc.accumulator(0.0)

    val rdd = sc.parallelize(1 to 100)
    rdd.foreach { x => {
      accumulator.add(x)
    }
    }
    accumulator.setValue(0.0)
    //0.0
    println(accumulator.value)
  }
}