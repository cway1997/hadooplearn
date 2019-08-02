package com.hpe.spark.sql.mysql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.sql.Connection
import java.sql.DriverManager

object SparkSQLReadMySQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkSQLReadMySQL")
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)


    /*   val map = Map(

       )*/

    val df =  sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://172.20.30.47:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable","spark")
      .option("user","root")
      .option("password","123")
      .load()

    sqlContext.udf.register("plugAge",(x:Int,str1:String,str2:String)=>x+10 + str1 + str2)

    df.registerTempTable("user_info")

    val xxrDF = sqlContext.sql("select plugAge(id,'~','zfg') as plusID,age from user_info where age < 40")
    /**
      * 1、
      * 2、
      * 3、
      */
    xxrDF.foreachPartition {  iterator => {
      Class.forName("com.mysql.jdbc.Driver")
      val connect = DriverManager.getConnection("jdbc:mysql://172.20.30.47:3306/test","root","123")
      val sql = "INSERT INTO xxr(id,name) VALUES(?,?)";
      val prestatem = connect.prepareStatement(sql)
      while(iterator.hasNext){
        val row = iterator.next()
        prestatem.setInt(1, row.getAs("age"))
        prestatem.setString(2, row.getAs("plusID"))
        prestatem.addBatch()
      }
      prestatem.executeBatch()
    } }
    df.show()

  }
}