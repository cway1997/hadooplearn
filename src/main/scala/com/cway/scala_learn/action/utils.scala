package com.cway.scala_learn.action

// 服务端发送客户端的消息格式
case class ServerMessage(msg: String)

// 客户端发送到服务单的消息格式 实现了serilize接口，可以用于网络传输
case class ClientMessage(msg: String)
