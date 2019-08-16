package com.cway.scala_learn

/**
  * @Author: Cway
  * @Description:
  * @Date: Create in 22:51 2019/8/6
  */
object Hello {
  def main(args: Array[String]): Unit = {
    // println("hello world")
    val p = new Persons("zs", 19)
    val person = Persons("wagnwu", 10)
    new Persons("cway", 22, "Man")
    println(p)
    println(person)
  }


  class Persons(xname: String, xage: Int) {
    val name = "zs"
    val age = xage
    var gender = "m"

    def this(name: String, age: Int, g: String) {
      this(name, age)
      gender = g
    }

    override def toString: String = {
      "[Name:" + name + ", Age:" + age + ", Gender:" + gender + "]"
    }
  }

  object Persons {
    def apply(name: String, age: Int) = {
      new Persons(name, age)
    }
  }

}
