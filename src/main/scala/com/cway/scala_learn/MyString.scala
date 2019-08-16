package com.cway.scala_learn

/**
  * @Author: Cway
  * @Description:
  * @Date: Create in 22:51 2019/8/6
  */
object MyString {
  def main(args: Array[String]): Unit = {
   var str = "AaDd"
   var str1 = "BbCc"
   val flag = str.compareToIgnoreCase("aadd")
   println(str.compareTo("AAaa"))

   println(flag)
  }
}
