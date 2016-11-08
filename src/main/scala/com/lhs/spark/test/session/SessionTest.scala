package com.lhs.spark.test.session

import org.apache.spark.sql.SparkSession

/**
 * Created by Administrator on 2016/11/8.
 */
object SessionTest {
  case class Person(name:String,age:Long)  //case class 不能在main方法红定义
  def main(args: Array[String]) {
    val spark = getSession
    import spark.implicits._


    val caseClassDS = Seq(Person("Andy",13)).toDF()
    caseClassDS.show()

    val primitiveDS = Seq(("primitive",11)).toDF("name","age")
    primitiveDS.show()
    spark.stop()
  }

  def getSession={
    SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
  }

  def jsonTest(spark:SparkSession): Unit = {
    import spark.implicits._
    val df = spark.read.json("F:\\spark\\examples\\src\\main\\resources\\people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name",$"age"+1).show()
    df.filter($"age">21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
  }
}
