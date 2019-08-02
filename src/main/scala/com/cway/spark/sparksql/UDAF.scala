package com.cway.spark.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 输入多条语句 输出一条
 */
object UDAF {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      .setAppName("SparkSQLReadMySQL")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
      
    val sqlContext = new SQLContext(sc)
      
    sqlContext.udf.register("myAverage", new UserDAF)

    val df = sqlContext.read.json("file:///d:/data/employees.json")
    df.registerTempTable("employees")
    df.show()
    df.printSchema()
    
    /**
     * 自定义一个UDAF函数，计算所有人的平均工资  avg()  UDAF
     */
    val result = sqlContext.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }
}

/**
 * 统计所有员工的平均工资:
 * 		1、统计所有的员工总数
 * 		2、统计所有的工资总数
 * 		3、工资总数/员工总数 = 平均工资
 */
class UserDAF extends UserDefinedAggregateFunction {
  //输入数据的类型
  def inputSchema: StructType = 
    StructType(StructField("inputColumn", LongType) :: Nil)

  
  
  /**
   *  初始化一个buffer，实际上这个buffer就是一个row对象，具备更新、根据索引查找等特性  
   */
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }
  
  
  // 将输入数据更新到buffer中    update === map combiner
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      //buffer的0号位置来统计员工的总工资
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      //buffer的1号位置 通过+1来统计员工数
      buffer(1) = buffer.getLong(1) + 1
    }
  }
  
  //在shuffle过程中计算的数据类型
  def bufferSchema: StructType = {
    StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }


  /**
   * reduce端的大合并
   * 		合并的是map端combiner的结果    buffer
   */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  
    //最终返回结果的数据类型
  def dataType: DataType = DoubleType
  
  // 函数是否在相同的输入上返回相同的输出
  def deterministic: Boolean = true
  
  
  // 返回最终结果  evalate返回值的类型必须与dataType指定的类型一致，否则报错
  def evaluate(buffer: Row) = buffer.getLong(0).toDouble / buffer.getLong(1) 
}