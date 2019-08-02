package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

/**
  * 作业：
  * 1、计算页面总数广播
  * 2、该用广播改成使用广播变量
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Fof")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    /**
      * 注意点：
      * 1、累积器只能在Driver端定义
      * 2、累加器只能在Driver端读取，不能再Executor读取
      * 3、累积器中的值只能Executor端修改
      */
    var errorAccumulator = sc.accumulator(0.0)
    /**
      * 当你的工程下有hdfs-site.xml,core-site.xml,
      * 如果你要计算的是本地文件，路径前缀加上file:///
      * 否则，他会任务你的这个路径是HDFS上的路径，
      * 近而给你报错file not found
      */
    val rdd = sc.textFile("d:/data/pagerank.txt")
    var linksRDD = rdd.map { x => {
      val splited = x.split("\t")
      val me = splited(0)
      val nodes = splited.drop(1)
      (me, Node(1.0, nodes))
    }
    }
    var flag = true
    while (flag) {
      linksRDD = linksRDD.flatMap(x => {
        val tpList = new ListBuffer[(String, Double)]
        //计算票面值
        val me = x._1
        val node = x._2
        val pr = node.pr
        val nodes = node.nodes
        val pmz = pr / nodes.length
        for (node <- nodes) {
          tpList.+=((node, pmz))
        }
        tpList
      })
        .reduceByKey(_ + _)
        .join(linksRDD)
        .map(x => {
          val me = x._1
          val jpr = x._2._1
          val node = x._2._2
          val zpr = (1 - 0.85) / 4 + 0.85 * jpr
          val err = math.abs((zpr - node.pr) / 4)
          node.pr = zpr
          errorAccumulator.add(err)
          (me, node)
        })
      linksRDD.foreach(x => {
        val me = x._1
        val node = x._2
        println(me + "\t" + node.pr)

      })
      if (errorAccumulator.value <= 0.1) flag = false
      errorAccumulator.setValue(0.0)
    }


  }
}

case class Node(var pr: Double, var nodes: Array[String])



