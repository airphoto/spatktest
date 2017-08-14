package com.lhs.spark.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SQLExample {
  case class Person(name:String,age:Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLExample").master("local[*]").getOrCreate()

//    runBasicDataFrameExample(spark)
    runJdbcDatasetExample(spark)
    spark.close()
  }

  def runBasicDataFrameExample(spark: SparkSession) = {
    import spark.implicits._
    val df = spark.read.json("F:\\tmpdata\\test\\resources\\people.json").as[Person]
    df.show()
  }

  def runJdbcDatasetExample(spark: SparkSession) = {
    import spark.implicits._

    val table = "(select * from city where ID<=1000) myCity"
    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/world")
      .option("dbtable",table)
      .option("user","root")
      .option("password","123456")
      .option("partitionColumn","ID")
      .option("lowerBound","100")
      .option("upperBound","4079")
      .option("numPartitions","4")
      .load()

//      jdbcDF.foreachPartition(it=>println("format-->"+it.size))
//    println(jdbcDF.count())



    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val predicates = (0 to 9).map(id=>s"ID % 10 = $id").toArray
    val predDF = spark.read.jdbc("jdbc:mysql://localhost:3306/world","city",predicates,connectionProperties)
//    predDF.foreachPartition(it=>println("predicates-->"+it.length))

//    val joindata = jdbcDF.join(predDF,jdbcDF("ID")===predDF("ID"),"left_outer")
////    println("size---->"+joindata.rdd.partitions.size)
//    println("all data count ->"+joindata.count)

    predDF.agg(Map("ID" -> "max", "Population" -> "avg")).show()


    import org.apache.spark.sql.functions._
    predDF.agg(max("ID").as("maxID"),mean("Population").as("mean")).show()

  }
}
