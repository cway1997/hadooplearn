package com.cway.spark.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.HashPartitioner

/**
 * 应用场景:
 * 		1、双11 11   广告     UpdateStateByKey来广告点击统计
 * 		2、浏览量统计    微博  博客  公众号... UpdateStateByKey
 */
object UpdateStateByKeyOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UpdateStateByKeyOperator").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("file:///d:/data/SparkStreaming/checkpoint")
    
    
    val initialRDD = ssc.sparkContext.makeRDD(
    Array(
      ("hello",100),
      ("bj",100),
       ("sh",100),
        ("gz",100)
      ),2  
    )  
    
    val initDStream = ssc.socketTextStream("172.20.30.5", 9999)
    initDStream
      .flatMap { _.split(" ") }
      .map((_,1))
      //elems:因为updateStateByKey 他会根据key来分组 elems表示的就是当前key所对应的那一堆value
      //beforeState:当前key在上一次计算完成后的状态
      /**
       * batch 0
       * hello bj
       * hello sh
       * hello sz
       * 		hello 0+1+1+1=3
       * 		
       * batch 1
       * hello bj
       * hello sh
       * 			hello [1,1]
       * 			bj [1]
       * 			sh [1]
       * 			hello 3+1+1=5
       * 			bj 1+1=2
       * 			sh 1+1=2
       * 
       * 更新函数
       * 
       *  requirement failed: The checkpoint directory has not been set. 
       *  Please set it by StreamingContext.checkpoint().
       *  
       *  没有设置检查点目录
       *  	updateStateByKey 会将每一个key对应的状态保存到内存中，但是内存不稳定，容易造成数据的丢失
       *  	所以强迫我们程序员必须设置一个磁盘目录，他会自动的将内存中的数据更新到磁盘上
       */
//      .updateStateByKey((elems:Seq[Int],beforeState:Option[Int])=>{
//        var initState = beforeState.getOrElse(0)
//        for(elem <- elems){
//          initState += elem
//        }
//        Option(initState)
//      }).print()
  
      .updateStateByKey((elems:Seq[Int],beforeState:Option[Int])=>{
        var initState = beforeState.getOrElse(0)
        for(elem <- elems){
          initState += elem
        }
        Option(initState)
        //HashPartitioner   key->HashPartitioner ->
      }, new HashPartitioner(2), initialRDD)
      .print()
      
      
    ssc.start()
    ssc.awaitTermination()
  }
}