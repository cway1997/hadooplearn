package com.cway.spark.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time

/**
 * ForeachRDD算子也是可以将DStream中的RDD抽取出来
 * 与transform不同的是ForeachRDD是一个output operation类算子
 */
object ForeachRDDOperator {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDOperator").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val initDStream = ssc.socketTextStream("172.20.30.5", 9999)

    
    initDStream.foreachRDD((rdd: RDD[String], time: Time) => {
      rdd
        .flatMap { _.split(" ") }
        .map((_, 1))
        .reduceByKey(_ + _)
       //foreachRDD中必须得有action类算子
         .foreach(println)
    })
    
    ssc.start
    ssc.awaitTermination()

  }
}