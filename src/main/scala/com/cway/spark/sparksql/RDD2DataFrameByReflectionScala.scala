package com.cway.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * 通过反射的方式将一个普通的RDD 转成DF
  *
  * 弊端：
  * 修改列名以及列的类型的时候，需要去修改代码 -> jar ->client ->spark-submit
  *
  */
object RDD2DataFrameByReflectionScala {

  case class Person(name111: String, age111: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf() //创建sparkConf对象
    conf.setAppName("My First Spark App") //设置应用程序的名称，在程序运行的监控页面可以看到名称
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    //用于包含RDD到DataFrame隐式转换操作
    import sqlContext.implicits._


    /**
      * 底层原理，直接调用toDF方法，
      * 实际上会通过反射的方式去调用Person对象的字段以及每一个字段的类型
      * 拿到每一个字段的名字和类型作为DF的列的名字和类型
      */
    val people = sc.textFile("Peoples.txt").map(_.split(",")).map(p => Person(p(1), p(2).trim.toInt)).toDF()

    people.show()
    people.registerTempTable("people")


    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 6 AND age <= 19")

    /**
      * 对dataFrame使用map算子后，返回类型是RDD<Row>
      * 通过索引号去row对象中取值
      */
    teenagers.map(t => "Name: " + t(0)).foreach(f = println)

    /**
      * 通过列名获取row对象中对应的值
      */
    teenagers.map(t => "Name: " + t.getAs[String]("name")).foreach(f = println)

    sc.stop()
  }
}
