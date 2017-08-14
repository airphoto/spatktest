package com.lhs.spark.dataset

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class Persion(name:String,age:Int)
object InteroperatingWithRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("InteroperatingWithRDD").master("local").getOrCreate()
    import spark.implicits._

    val peopleDF = spark.read
      .textFile("F:\\tmpdata\\test\\resources\\people.txt")
      .map(_.split(","))
      .map(att=>Persion(att(0),att(1).trim.toInt))
      .toDF()

    peopleDF.show()


    spark.stop()
  }
}
