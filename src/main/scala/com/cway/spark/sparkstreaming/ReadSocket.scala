package com.cway.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * sparkStreaming处理socket中的数据
 * 注意点：
 * 		模拟线程至少需要2个  1个用于接受数据，一个用于处理数据
 */
object ReadSocket {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("ReadSocket")
    
    
    val ssc = new StreamingContext(conf,Seconds(5))
    
    val initDStream:ReceiverInputDStream[String] = ssc.socketTextStream("172.20.30.5", 9999)
    
    initDStream
        .flatMap { _.split(" ") }
        .map((_,1))
        .reduceByKey(_+_)
        .print
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}