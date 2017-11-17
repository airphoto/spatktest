package com.lhs.spark.core.session

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StringType, StructField}

/**
 * Created by Administrator on 2016/11/8.
 */
object SessionTest {
  case class Person(name:String,age:Long)
  def main(args: Array[String]) {
    val spark = getSession
    import spark.implicits._

    val sql = "(select count(*) from itim_wuxi2.call_log_201611) tmp" //必须做为一个table

    val jdbcDF = spark.read.format("jdbc").option("url","jdbc:mysql://10.111.64.184:3306/test?useSSL=false").option("user","shop_test").option("password","XL@2016mysql").option("dbtable","user_info").option("numPartitions","10").load()
    jdbcDF.show()
    spark.stop()
  }

  def specifySchema(spark:SparkSession): Unit ={

    import spark.implicits._
    val peopleRDD = spark.sparkContext
      .textFile("F:\\spark\\examples\\src\\main\\resources\\people.txt")

    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName,StringType,nullable = true))

    val shema = StructType(fields)

//    val rowRDD = peopleRDD.map(_.split(","))
//      .map(attribute => Row(attribute(0),attribute(1).trim))
//
//    val peopleDF = spark.createDataFrame(rowRDD,shema)
//
//    peopleDF.createOrReplaceTempView("people")

    val results = spark.sql("select name from people")

    results.map(attribute => "name => " + attribute.get(0)).show()
  }

  def refSchema(spark:SparkSession)={
    import spark.implicits._
    val peopleDF = spark.sparkContext
      .textFile("F:\\spark\\examples\\src\\main\\resources\\people.txt")
//      .map(_.split(" "))
//      .map(x=>x.split(","))
//      .map(attributes => Person(attributes(0),attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("select name,age from people")

    teenagersDF.map(teenager=> s"name => "+teenager(0)).show()
    teenagersDF.map(teenager => "name "+teenager.getAs[Person]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String,Any]];

    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name","age"))).collect().foreach(println)

  }

  def createDatasets(spark:SparkSession)={
    import spark.implicits._
    val caseClassDS = Seq(Person("Andy",13)).toDF()
    caseClassDS.show()

    val primitiveDS = Seq(("primitive",11)).toDF("name","age")
    primitiveDS.show()

    val peopoleDS = spark.read.json("").as[Person]
    peopoleDS.show()
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
