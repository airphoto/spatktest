package com.lhs.spark.dataset

import org.apache.spark.sql.SparkSession

case class Person(name:String,age:Int)
object CreateDataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("create").master("local").getOrCreate()
    import spark.implicits._
    val primitiveDS = Seq(1,2,3).toDS()
    val dsct = primitiveDS.collect()


    val caseDS = Seq(Person("case",11)).toDS()
    caseDS.show()

    spark.stop()
  }
}
