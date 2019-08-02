package com.cway.spark.sparkstreaming

import scala.collection.mutable.Queue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 通过将一个个RDD添加到DStream中
 * Discretized stream
 * 
 * 这段代码没有实际意义，只是为了让大家更好的理解SparkStreaming
 */
object QueueStreamTest  {
  def main(args: Array[String]): Unit = {
    
     val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
     //StreamingContext对象中实际封装的依然是sc
     ssc.sparkContext.setLogLevel("WARN")

    val rddQueue = new Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()
    ssc.start()

    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(5000)
    }
    ssc.stop()
  }
}