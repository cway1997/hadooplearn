package com.cway.scala_learn.action

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

object ChineseChess extends App {
  private val ChineseChessActorSystem = ActorSystem("Chinese-chess")
  private val p2 = ChineseChessActorSystem.actorOf(Props[player2Actor], "play2")
  private val p1 = ChineseChessActorSystem.actorOf(Props(new player1Actor(p2)), "play1")

  p2 ! "start"
  p1 ! "start"
}

class player1Actor(val p2:ActorRef) extends Actor{
  override def receive: Receive = {
    case "start" => {
      println("棋圣：I'm OK !")
      p2 ! "该你了"
    }
    case "将军" => {
      println("棋圣：你真猛！")
      Thread.sleep(1000)
      p2 ! "该你了"
    }
  }
}

class player2Actor extends Actor{
  override def receive: Receive = {
    case "start" => {
      println("棋仙说：I'm OK !")
    }
    case "该你了" => {
      println("棋仙：那必须滴！")
      Thread.sleep(1000)
      // 这个sender() , 其实就是对ActorRef的一个引用。它指的是给发送“该你了”的这个对象本身
      sender() ! "将军"
    }
  }
}

