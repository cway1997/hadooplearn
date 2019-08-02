package com.cway.spark.sparkstreaming

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Set
import kafka.serializer.StringDecoder

object SparkStreamingOnKafkaDirect {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreamingOnKafkaDirect").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
//    ssc.checkpoint("")
    
    val brokers = "node01:9092,node02:9092,node03:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = Set("mdj","mdj")
    
    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    
     inputDStream
      .flatMap { _._2.split(" ") }
      .map { (_,1) }
      .reduceByKey(_+_)
      .print()
      
    ssc.start()
    ssc.awaitTermination()
  }
}