package com.cway.spark.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object DataFrameOpsFromJsonRdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    val infos = Array("{'name':'zhangsan', 'age':55}","{'name':'lisi', 'age':30}","{'name':'wangwu', 'age':19}")
    val scores = Array("{'name':'zhangsan', 'score':155}","{'name':'lisi', 'score':130}")

    /**
      * infoRdd  jsonRDD
      */
    val infoRdd = sc.parallelize(infos)
    val scoreRdd = sc.parallelize(scores)
    
    val infoDF = sqlContext.read.json(infoRdd)
    val scoreDF = sqlContext.read.json(scoreRdd)
    
//    infoDF.registerTempTable("people")
    
 
    infoDF.join(scoreDF, infoDF("name")===(scoreDF("name"))).select(infoDF("name"),infoDF("age"),scoreDF("score")).show()

    infoDF.registerTempTable("info")
    scoreDF.registerTempTable("score")
    val sql = "SELECT a.name,a.age,b.score FROM info a JOIN score b ON (a.name=b.name)"

    sqlContext.sql(sql).show()
    
    sc.stop()
    
    
   /* df.show()

    df.printSchema()
    
    df.select("name").show()
    
    df.select(df("name"), df("age")+10).show()
    
    df.filter(df("age")>10).show()
    
    df.groupBy("age").count.show()*/
  }
}