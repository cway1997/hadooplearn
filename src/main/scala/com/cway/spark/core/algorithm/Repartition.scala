package com.cway.spark.core.algorithm

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object Repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Repartition").setMaster("local")
    val sc = new SparkContext(conf)

    val facePowerList = new ListBuffer[(String, Int)]()
    for (i <- 1 to 10) {
      facePowerList.+=(("AngelaBaby" + i, 100 * i))
    }
    val facePowerRDD = sc.makeRDD(facePowerList, 4)
    //查看分区信息
    facePowerRDD.mapPartitionsWithIndex((index, iterator) => {
      println("partitionId" + index)
      while (iterator.hasNext) {
        println(iterator.next())
      }
      iterator
    }).count()

    //增加RDD的分区数
    val coalesceRDD1 = facePowerRDD.coalesce(6, true)
    println("coalesceRDD2.getNumPartitions:" + coalesceRDD1.getNumPartitions)
    coalesceRDD1.mapPartitionsWithIndex((index, iterator) => {
      println("partitionId" + index)
      while (iterator.hasNext) {
        println(iterator.next())
      }
      iterator
    }).count()

    // 减少RDD的分区数
    facePowerRDD.repartition(2)
      .mapPartitionsWithIndex((index, iterator) => {
        println("partitionId" + index)
        while (iterator.hasNext) {
          println(iterator.next())
        }
        iterator
      }).count

    sc.stop()
  }
}