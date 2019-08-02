package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object OtherOperator {
  def main(args: Array[String]): Unit = {
    //union合并
    val conf = new SparkConf()
    conf.setAppName("OtherOperator")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(1 to 10, 3)
    val rdd2 = sc.makeRDD(11 to 20,3)
    /**
     * 他只是將rdd1与rdd2在逻辑上进行合并，并不会真正进行数据的合并以传输
     *	羽泉组合  凤凰传奇 筷子兄弟  摩登兄弟
     *  join 横向关联  key相同的才能关联
     */
    val unionRDD = rdd1.union(rdd2)
    println(unionRDD.getNumPartitions)
    
    /**
     * zip:将两个RDD进行横向合并  但是zip是对应位置合并
     * 比如  非KV合适的RDD1 RDD2 zip KV格式的RDD
     * 注意点：
     * 		1、要进行zip的两个RDD的元素数必须一致
     * 		2、要进行zip的两个RDD的分区数必须一致
     */
    val zipRDD = rdd1.zip(rdd2)
    zipRDD.foreach(println)
    
    /**
     * zipWitIndex  给RDD中的每一个元素加上一个唯一的索引号，非KV的RDD变成了KV格式的RDD
     */
    val zipWithIndexRDD = rdd1.zipWithIndex()
    zipWithIndexRDD.foreach(println)
    zipWithIndexRDD.map(_.swap).lookup(2).foreach(println)
    
    
    /**
     * zipWithUniqueId:给RDD中的每一个元素加上一个唯一的索引号,非KV的RDD变成了KV格式的RDD
     * 每一个分区的第一个元素的索引号就是当前分区的分区号
     * 每一个分区的第二个元素的索引号就是第一个元素+分区数
     * step步长=分区数
     */
    rdd1
      .zipWithUniqueId()
      .mapPartitions(iterator=>{
        while(iterator.hasNext){
          println(iterator.next)
        }
        iterator
      }).count()
        
     /**
      * take(n)取这个RDD中前N个元素
      * action类算子
      * first
      */
      rdd1.take(5).foreach(println)
      //first = take(1)
      println(rdd1.first())
  }
}