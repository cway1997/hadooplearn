package com.cway.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingOnKafkaReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreamingOnKafkaReceiver").setMaster("local[*]")
    //开启WAL机制   file:///d:/data/sparkstreaming
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("file:///d:/data/sparkstreaming/WAL")
    
    val topics = Map("mdj" -> 3)
    val zkQuorum = "node02:2181,node03:2181,node04:2181"
    
    val inputDStream = KafkaUtils.createStream(ssc, zkQuorum, "abc", topics,StorageLevel.MEMORY_ONLY)
     inputDStream
      .flatMap { _._2.split(" ") }
      .map { (_,1) }
      .reduceByKey(_+_)
      .print() 
   
      
    ssc.start()
    ssc.awaitTermination()
  }
}