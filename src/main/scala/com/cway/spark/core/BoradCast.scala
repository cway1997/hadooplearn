package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * 注意点：
  * 1、广播变量只能够在Driver端定义
  * 2、广播变量压根就没有提供修改数据的方法，所以在Executor端只能够读取，不能修改
  */
object BoradCast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("BoradCast")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array("Angelababy", "Dilireba", "wangfei", "xuruyun"))

    val blackNames = List("wangfei", "xuruyun")

    val broadcast = sc.broadcast(blackNames)

    rdd.filter { x => !broadcast.value.contains(x) }.foreach(println)

  }
}