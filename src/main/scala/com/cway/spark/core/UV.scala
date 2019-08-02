package com.cway.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

object UV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    // -Dspark.master=spark://192.168.201.101:7077
    conf.setMaster("local")
        .setAppName("UV")
//        .setSparkHome("/usr/aboutyun/spark-2.3.3")
    val sc = new SparkContext(conf)

//    val path = "d:/logdata"
    val logRdd = sc.textFile("file:///d:/logdata")
    val filedRDD = logRdd.map { x => {
      val splited = x.split("\t")
      splited
    }
    }

    val filterRDD = filedRDD.filter { x => x.length == 5 }
    val dateRDD = filterRDD.map { x => {
      val timestamp = x(2).toLong
      val date = new Date(timestamp)
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val dateStr = format.format(date)
      x(2) = dateStr
      (x(2), x(1) + "-" + x(3))
    }
    }

    val groupRDD: RDD[(String, Iterable[String])] = dateRDD.groupByKey()
    val UVRDD = groupRDD.flatMap(x => {
      val date = x._1
      val values = x._2
      val set = new HashSet[String]
      for (elem <- values) {
        set.add(elem)
      }

      val map = new HashMap[String, Int]
      for (elem <- set) {
        val splited = elem.split("-")
        //        val userId = splited(0)
        val pageId = splited(1)
        if (map.contains(pageId)) {
          map.put(pageId, map.get(pageId).get + 1)
        } else {
          map.put(pageId, 1)
        }
      }

      val list = new ListBuffer[(String, Int)]
      for (elem <- map) {
        val pageId = elem._1
        val count = elem._2
        list.+=((date + "-" + pageId, count))
      }
      list
    })
    val UVSortRDD = UVRDD.sortBy(_._2, false)
    UVSortRDD.saveAsTextFile("file:///d:/uv")
    sc.stop()
  }
}



