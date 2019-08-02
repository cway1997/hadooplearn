package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

/**
 * 复习：stage的时候，为啥要切割成一个个的stage？
 * 		以为就是为了得到一个个task，每一个task的计算模式pipeline
 * 	stage的并行度：是由这个stage中最后一个RDD（final RDD）的分区数决定的
 * 						这个stage中第一个RDD的分区数只能够影响stage的并行度，而不能决定
 * 	说白了  partition的个数 直接影响并行度
 * 提高并行度：增加RDD的分区数
 * coalesce
 * repartition
 * 减少分区使用coalesce算子
 * 增加分区使用coalesce(..,true)或者repartition
 * 
 */
object Repartition {
  def main(args: Array[String]): Unit = {
    /**
     * local[10] 代码在本机使用10个线程来模拟spark的执行
     * local: 使用1个线程来模拟
     * local[*]:  你的电脑还剩下几个剩余的core，那么就启动多少个线程来模拟
     */
    
    val conf = new SparkConf().setMaster("local").setAppName("Repartition")
    val sc = new SparkContext(conf)
    val facePowerList = new ListBuffer[(String,Int)]()
    for(i <- 1 to 12){
      facePowerList.+=(("Angelababy"+i , 100 * i))
    }
    val facePowerRDD = sc.makeRDD(facePowerList,4)
    facePowerRDD.mapPartitionsWithIndex((index,iterator)=>{
      println("partitionId" + index)
      while(iterator.hasNext){
        println(iterator.next)
      }
      iterator
    }).count()
    
    //增加RDD的分区数
    val coalesceRDD1 = facePowerRDD.coalesce(6, true)
    
    println("coalesceRDD1.getNumPartitions:" + coalesceRDD1.getNumPartitions)
    coalesceRDD1.mapPartitionsWithIndex((index,iterator)=>{
      println("partitionId" + index)
      while(iterator.hasNext){
        println(iterator.next)
      }
      iterator
    }).count()
    
    /**
     * 分区的过程会产生shuffle
     * repartition(numPartitons) = coalesce(numPartitons, true) 
     */
     val coalesceRDD2 = facePowerRDD.repartition(6)
     println("coalesceRDD2.getNumPartitions:" + coalesceRDD2.getNumPartitions)
 
     /**
      * 减少分区  有没有必要产生shuffle?
      * 没有必要
      * 非要使用带有shuffle的重分区算子也可以，但是有效率问题
      */
     facePowerRDD
       .coalesce(2, false)
       .mapPartitionsWithIndex((index,iterator)=>{
          println("partitionId" + index)
          while(iterator.hasNext){
            println(iterator.next)
          }
          iterator
        }).count()
        
      facePowerRDD
//       .coalesce(2, true)
       .repartition(2)
       .mapPartitionsWithIndex((index,iterator)=>{
          println("partitionId" + index)
          while(iterator.hasNext){
            println(iterator.next)
          }
          iterator
        }).count()
    
    //Driver会通知master，将集群上的Executor全部kill掉，近而释放了资源
    sc.stop()
  }
}