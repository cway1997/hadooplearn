import java.util.Date

object Person {

  def main(args: Array[String]): Unit = {
    println(fun1(1, 3))
    println(fun2(5))
    println(fun3(num1 = 50))
    println(fun4(1, 2))
    println(fun5(1.2, 2.4, 3.6, 4.8, 6.0))
    println(fun6(1,1))
    logBoundDate("log1")
    logBoundDate("log2")
    println(fun7(f1,5))
    println(fun8()(2,1))
    println(fun9(f1)(2.0,4.0))
  }

  /**
   * 普通函数
   */
  def fun1(num1: Int, num2: Int) = num1 + num2

  /**
   * 递归函数
   * 		必须说明返回值的类型
   */
  def fun2(num: Int): Int = {
    if (num == 1 || num == 0) 1
    else num * fun2(num - 1)
  }

  /**
   * 默认参数的函数
   */
  def fun3(num: Int = 10, num1: Int) = num + num1

  /**
   * 嵌套函数
   */

  def fun4(num: Int, num1: Int) = {
    def fun(num2: Int) = {
      num2 / 2
    }
    num + fun(num1)
  }

  /**
   * 可变参数个数的函数
   */
  def fun5(args: Double*) = {
    var sum = 0.0
    for (arg <- args) sum += arg
    sum
  }

  /**
   * 匿名函数
   */
  var fun6 = (num: Int, num1: Int) => num + num1

  /**
   * 偏应用函数
   */
  def log(date:Date,content:String)={
    println("date:" + date + "\tcontent:" +content)
  }
  var date = new Date();
  var logBoundDate = log(date,_:String)
  
  /**
   * 高阶函数
   */
  //函数的参数是函数
  def f1(num:Int,num1:Int) = num+num1
  def fun7(f1:(Int,Int)=>Int,num:Int)={
    f1(num,2)
  }
  
  //函数的返回值是函数
  def fun8():(Int,Int)=>Int = {
//    def temp(n1:Int,n2:Int) = n1 - n2
//    temp
    (n1:Int,n2:Int)=>n1 - n2
  }

  //函数的参数和返回值都是参数
  def fun9(f1:(Int,Int)=>Int):(Double,Double)=>Double = {
    println(f1(1,3))
    (n1:Double,n2:Double)=>n1/n2
  }
}

