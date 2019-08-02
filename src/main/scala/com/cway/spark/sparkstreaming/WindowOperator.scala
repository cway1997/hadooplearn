package com.cway.spark.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

/**
 * The window duration of windowed DStream 
 * (12000 ms) must be a multiple of the slide
 *  duration of parent DStream (5000 ms)
 */
object WindowOperator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WindowOperator").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("file:///d:/data/SparkStreaming/window")
    ssc.sparkContext.setLogLevel("WARN")
    
    val initDStream = ssc.socketTextStream("172.20.30.5", 9999)
    
    initDStream
        .flatMap { _.split(" ") }
        .map((_,1))
//        .reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2, Seconds(15), Seconds(10))
        .reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2, (v1:Int,v2:Int)=>v1-v2, Seconds(15), Seconds(10))
        .print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}