package com.cway.spark.sparkstreaming

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Set
import kafka.serializer.StringDecoder

object FilterBlackName {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreamingOnKafkaDirect").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
//    ssc.checkpoint("")
    ssc.sparkContext.setLogLevel("WARN")
    val blackNamesRDD = ssc.sparkContext.makeRDD(Array(("hanhong",55),("fengjie",18)))
    
    val brokers = "node01:9092,node02:9092,node03:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = Set("mdj","mdj")
    
    
    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    
    inputDStream.foreachRDD(rdd=>{
      //foreachRDD中必须至少得有一个action算子触发执行
      rdd.map(x=>{
        val splited = x._2.split(" ")
        (splited(1),null)
      })
      .join(blackNamesRDD)
      .foreach(x=>{
        println(x._1+"==="+x._2._2)
      })
    })
      
    ssc.start()
    ssc.awaitTermination()
  }
}