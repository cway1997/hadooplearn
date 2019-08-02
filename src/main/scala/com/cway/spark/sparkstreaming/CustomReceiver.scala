package com.cway.spark.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * 教写代码套路
 * 		自定义监控器来监控mysql增量数据
 * 		1-5s
 * 			id  1-10
 * 		6-10s
 * 			id>10   读取过来
 * 				
 * 
 */
object CustomReceiver {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //实际自定义的接收器  依然接受的是socket中的数据  
    val lines = ssc.receiverStream(new CustomReceiver("172.20.30.5", 9999))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    socket = null;
  }

  var socket: Socket = null
  private def receive() {
   var userInput: String = null
   try {
     socket = new Socket(host, port)
     val reader = new BufferedReader(
       new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
     userInput = reader.readLine()
     
     while(!isStopped && userInput != null) {
       store(userInput)
//       val splited = userInput.split(" ")
//       store(splited.iterator)
       
       userInput = reader.readLine()
     }
     reader.close()
     socket.close()
     if( userInput == null){
        restart("try connecting to socket")
     }
   } catch {
     case e: java.net.ConnectException =>
       restart("Error connecting to " + host + ":" + port, e)
     case t: Throwable =>
       restart("Error receiving data", t)
   }
  }
}