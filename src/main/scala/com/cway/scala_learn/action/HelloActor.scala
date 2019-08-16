package com.cway.scala_learn.action

import akka.actor.{Actor, ActorSystem, Props}

import scala.io.StdIn


//object scala07 {
//  def main(args: Array[String]): Unit = {
//    val actor2 = new DreamGirlActor()
//    val actor1 = new MyActor(actor2)
//    actor1.start()
//    actor2.start()
//  }
//}

class HelloActor extends Actor{
  // 重写接受消息的偏函数，其功能是接受消息并处理
  override def receive: Receive = {
    case "你好帅" => println("竟然说实话，我喜欢你这种人！")
    case "丑八怪" => println("滚犊子！")
    case "stop" => {
      context.stop(self)          // 停止自己的actorRed
      context.system.terminate()  // 关闭ActorSystem，即关闭其内部线程池(ExcutorService)
    }
  }
}

object HelloActor{
  // 创建线程池对象MyFactory，用来创建actor的对象
  private val MyFactory = ActorSystem("myFactory")    //里面的"myFactory"参数为线程池的名称
  // 通过MyFactory.actorOf方法来创建一个actor，注意，Props方法的第一个参数需要传递我们自定义的HelloActor类
  // 第二个参数给actor起个名字
  private val helloActorRef = MyFactory.actorOf(Props[HelloActor], "helloActor")

  def main(args: Array[String]): Unit = {
    var flag = true
    while (flag){
      print("请输入你想发送的消息：")
      val consoleLine:String = StdIn.readLine()
      // 使用helloActorRef来给自己发送消息，helloActorRef有一个叫做感叹号("!")的方法来发送消息
      helloActorRef ! consoleLine
      if (consoleLine.equals("stop")){
        flag=false
        println("End")
      }
      // 为了不让while的运行速度在receive方法之上，我们可以让他休眠0.1秒
      Thread.sleep(100)
    }
  }
}

/*
class MyActor(actor:Actor) extends Actor{
  actor ! Message(this,"hello")
  def act() = {
    while (true) {
    	receive{
    	  case s:String => {
    	    println(s)
    	    actor ! "nice"
    	  }
    	}
    }
  }
}

class DreamGirlActor extends Actor{
  def act() = {
    while (true) {
      receive{
        case m:Message => {
          println(m.msg)
          m.actor ! "hey boy"
        }
        case s:String => println(s)
      }
    }
  }
}

case class Message(actor:Actor,msg:String)*/
