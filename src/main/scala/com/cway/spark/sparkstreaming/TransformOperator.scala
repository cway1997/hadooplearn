package com.cway.spark.sparkstreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD

/**
 * 我们之前说过DStream中封装的是RDD
 * transform算子能够将DStream中的RDD抽取出来
 * DStream->RDD->丰富的operator
 * 						 ->DF->view->SQL
 */
object TransformOperator {
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    
    val initDStream = ssc.socketTextStream("172.20.30.5", 9999)
    
    /**
     * rdd：从DStream中抽取出来的RDD
     * time就是时间戳
     */
    val restDStream = initDStream.transform((rdd:RDD[String],time:Time) =>{
      rdd
        .flatMap { _.split(" ") }
        .map((_,1))
        .reduceByKey(_+_)
    })
    
    /**
     * 在SparkStreaming Application中必须至少有一个output operator类算子
     */
    restDStream.print
    ssc.start
    ssc.awaitTermination()
    
  }
}