package com.cway.spark.core.algorithm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("combineByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(
      ("A", 1),
      ("A", 2),
      ("C", 4),
      ("B", 9),
      ("D", 2),
      ("D", 5),
      ("C", 3),
      ("C", 7),
      ("A", 5),
      ("A", 4),
      ("B", 3),
      ("E", 2)))

    //combineByKey模拟groupByKey
    rdd.combineByKey(x => {
      val listBuffer = new ListBuffer[Int]()
      listBuffer.+=(x)
    }, (x: ListBuffer[Int], y: Int) => {
      x.+=(y)
    }, (x: ListBuffer[Int], y: ListBuffer[Int]) => {
      x.++=(y)
    }).foreach(println)

    //combineByKey模拟reduceByKey
    rdd.combineByKey(x => x, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y).foreach(println)

    sc.stop()
  }

}