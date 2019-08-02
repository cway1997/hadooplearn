package com.cway.spark.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 动态创建schema的方式 将一个普通的RDD转成DF
 */
object RDD2DataFrameByProgrammatically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    
    val people = sc.textFile("scores.txt")
    
    val schemaString = "clazzaa:String scoreaa:Integer"
    //如果schema中制定了除String以外别的类型   在构建rowRDD的时候要注意指定类型     例如： p(2).toInt 
    val rowRDD = people.map(_.split("\t")).map(p => Row(p(0), p(1).toInt))
    
    /**
     * 创建结构信息
     */
    val schema =
      StructType(
           schemaString.split(" ")
             .map(fieldName => 
                 StructField(fieldName.split(":")(0),
                             if (fieldName.split(":")(1).equals("String")) StringType else IntegerType,
                             true)
                 )
           )
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.printSchema()
    peopleDataFrame.show()
   /* peopleDataFrame.registerTempTable("clazzScore")
    val results = sqlContext.sql("SELECT score,clazz FROM clazzScore")
    //  results.map(t => "age: " + t(0)).collect().foreach(println)
    results.map(t => "clazz: " + t.getAs[String]("clazz")+"\tscore:"+t.getAs[Integer]("score")).foreach(println)*/
    
  }
}












