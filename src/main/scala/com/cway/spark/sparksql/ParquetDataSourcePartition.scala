package com.cway.spark.sparksql

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * parquet格式的文件是一个压缩格式，他会更少的节约磁盘空间
 * 直接打开是看不懂的，通过hive SParkSQL可以读懂
 * 应用面很广
 */
object ParquetDataSourcePartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GenericLoadSave")
      .setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//    val sqlContext = new SQLContext(sc)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    val df = sqlContext
              .read
              .format("parquet")
              .load("file:///D:\\data\\users.parquet")
    df.show()
    df.printSchema()
    /**
     * SparkSQL中可以对parquet格式的文件进行自动推测分区
     * data
     * 		gender=female
     * 				country=US
     * 						数据
     * 				country=US
     * 						数据
     * 		genfer=male
     * 				country=US
     * 						数据
     * 				country=US
     * 						数据
     */
//    val ddff = sqlContext.read.json("D:\\data\\parquetdata")
    
    df.write.mode(SaveMode.Ignore).format("json").save("file:///d:\\data\\sparkuser.json")
    df.show()
    sc.stop()
  }
}







