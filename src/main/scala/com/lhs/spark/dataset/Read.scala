package com.lhs.spark.dataset

import org.apache.spark.sql.functions._
//import com.lhs.utils.{CaseClasses, HdfsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class Test(nam:String="test",age:Int)

object Read {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local[*]").appName("read").getOrCreate()
//    val outpath = "F:\\tmpdata\\test\\result\\people.csv"
//    import spark.implicits._
//    val rdd = spark.sparkContext.textFile("F:\\tmpdata\\test\\resources\\people.txt") //rdd的类型为DataSet[String]
//
//    val schemaString = "name,age"
//
//    val fields = schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true))
//
//    val schema = StructType(fields)
//
//    val rowRDD = rdd.map(_.split(",")).map(attributes=>Row(attributes(0),attributes(1).trim))
//
//    val peopleDF = spark.createDataFrame(rowRDD,schema)
//    HdfsUtil.delIfExists(outpath,spark)

//    readByFormat(spark)
import spark.implicits._
    val df = spark.createDataset(Seq(Test("1",2),Test("2",2),Test(age = 1)))
    val df2 = spark.createDataset(Seq((1,3))).toDF("name","books")
//    val df3 = df.join(df2,"name")

    df.show(false)
    df.printSchema()
//
//    df2.show(false)
//    df2.printSchema()
//
//    df3.show(false)
//    df3.printSchema()


    spark.stop()
  }
//
//  def readByFormat(sparkSession: SparkSession): Unit ={
//    import sparkSession.implicits._
//    val formatDs = sparkSession.read.format("text").load("F:\\tmpdata\\test\\resources\\people.txt")
//    val personDS = formatDs.map(x=>x.getAs[String]("value").split(",")).map(attr=>CaseClasses.Person(attr(0),attr(1).trim.toInt)).toDF()
//    broadcast(personDS)
//
//    personDS.show()
//  }
//
//  def readBySparkContext(sparkSession: SparkSession): Unit ={
//    import sparkSession.implicits._
//    val rdd = sparkSession.sparkContext.textFile("F:\\tmpdata\\test\\resources\\people.txt")
//    val rddDS = rdd.toDS()
//    sparkSession.sqlContext.sql("")
//  }
}
