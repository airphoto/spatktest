package com.lhs.spark.dataset

import com.lhs.utils.{CaseClasses, HdfsUtil}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Read {
  def main(args: Array[String]): Unit = {
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
    readBySparkContext(spark)
    spark.stop()
  }

  def readByFormat(sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    val formatDs = sparkSession.read.format("text").load("F:\\tmpdata\\test\\resources\\people.txt")
    val personDS = formatDs.map(x=>x.getAs[String]("value").split(",")).map(attr=>CaseClasses.Person(attr(0),attr(1).trim.toInt)).toDF()
    personDS.show()
  }

  def readBySparkContext(sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    val rdd = sparkSession.sparkContext.textFile("F:\\tmpdata\\test\\resources\\people.txt")
    val rddDS = rdd.toDS()
    rddDS.show()
  }
}
