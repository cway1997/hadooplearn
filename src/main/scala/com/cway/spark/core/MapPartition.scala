package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * MapPartition:遍历的单位是每一个partition
  * 遍历原理：将每一个partition的数据先加载到内存，然后在一条一条遍历
  * Map：
  * 遍历单位是每一条记录
  * rdd.map(x=>{......})
  * 场景：将RDD的数据写入到mysql oracle 应该选择哪一个算子来遍历RDD呢？
  * map？mapPartition？
  */
object MapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapPartition")
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(1 to 100, 10)

    /**
      * 创建数据库连接太多
      * 1、通过数据库连接池  不可行
      * 2、批量处理   （1）批太大 （2）Executor中使用的是Driver端中的链接（socket类型的链接是无法序列化的）
      * 3、使用mapPartition算子
      */
    println("创建连接")
    rdd.map { x => {
      println("拼接sql语句")
    }
    }.count()
    println("提交")


    //批不会太大，数据库连接又不会太多    数据库连接与partition个数一样
    rdd.mapPartitions((elems: Iterator[Int]) => {
      println("创建连接")
      while (elems.hasNext) {
        println("拼接SQL语句 " + elems.next)
      }
      println("提交")
      elems
    })


    /**
      * MapPartitionWithIndex
      * 在遍历每一个partition的时候能够拿到每一个分区的ID号
      * 这个算子一般用于测试环境
      */
    rdd.mapPartitionsWithIndex((index: Int, iterator: Iterator[Int]) => {
      println("partitonId: " + index)
      while (iterator.hasNext) {
        println(iterator.next)
      }
      iterator
    }).count()

    //获取RDD的分区数
    val partitionNum1 = rdd.getNumPartitions
    val partitionNum2 = rdd.partitions.length
    println(partitionNum1 + "===" + partitionNum2)

    /**
      * foreach ： 遍历单位是单条
      * foreachPartition vs mapPartition ： 最大的区别就是action transformation类算子区别
      */

    sc.stop()
  }
}