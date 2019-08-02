package com.cway.spark.core.algorithm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OthersOperator {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("OthersOperator").setMaster("local")
    val sc = new SparkContext(conf)

    // union合并 将两个rdd合并成一个rdd,分区数量不变
    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = sc.makeRDD(11 to 20, 3)
    val rdd3 = sc.parallelize(6 to 15, 3)

    val unionRDD = rdd1.union(rdd2)
    unionRDD.mapPartitionsWithIndex((index, iterator) => {
      println("partitionId" + index)
      while (iterator.hasNext) {
        println(iterator.next())
      }
      iterator
    }).count
    println(unionRDD.getNumPartitions)

    /**
     *  zip：将两个RDD进行横向合并 但zip是对应位置合并
     *  比如，非KV格式的RDD1 RDD2 zip KV格式RDD
     *  注意点：
     *  	1、要进行zip的两个RDD的元素数必须一致
     *  	2、要进行zip的两个RDD的分区数必须一致
     */
    val zipRDD = rdd1.zip(rdd2)
    zipRDD.mapPartitionsWithIndex((index, iterator) => {
      println("partitionId" + index)
      while (iterator.hasNext) {
        println(iterator.next())
      }
      iterator
    }).count
    println(zipRDD.getNumPartitions)

    /**
     *  zipWithIndex 给RDD中的每一个元素加上一个唯一的索引号，非KV格式的RDD变成了KV格式的RDD
     *  注意：用此方法添加的索引号在V值，即x._2位置
     */
    println("-----------zipWithIndex---------------")
    val zipWithIndex = rdd1.zipWithIndex()
    zipWithIndex.mapPartitionsWithIndex((index, iterator) => {
      println("partitionId" + index)
      while (iterator.hasNext) {
        println(iterator.next())
      }
      iterator
    }).count

    /**
     *  zipWithUniqueId：给RDD中的每一个元素加上一个唯一的索引号，非KV格式的RDD变成了KV格式的RDD
     *  每一个分区的第一个元素的索引号就是当前分区的分区号
     *  每一个分区的第二个元素的索引号就是第一个元素+分区数
     *  step步长=分区数
     *  注意：用此方法添加的索引号在V值，即x._2位置
     */
    println("------------zipWithUniqueId--------------")
    rdd1.zipWithUniqueId()
      .mapPartitionsWithIndex((index, iterator) => {
        println("partitionId" + index)
        while (iterator.hasNext) {
          println(iterator.next())
        }
        iterator
      }).count

    /**
     *  take(n) 取这个RDD的前n个元素
     *  action类算子
     *  first = take(1)
     */
    rdd1.take(5).foreach(println)
    println(rdd1.first())

    sc.stop()
  }
}