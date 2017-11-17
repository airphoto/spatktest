package com.lhs.spark.dataset

import com.lhs.spark.dataset.Messages.GamePlay
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object DatasetAPITest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("DatasetAPITest").getOrCreate()
    import spark.implicits._

    val df = spark.read.textFile("F:\\tmpdata\\test\\common\\first")
//    val par = spark.read.parquet("")

//    df.map(x=>x.split("\\|").length).collect().foreach(println)

    val washData = df.mapPartitions(it=>{
      val sp = it.asInstanceOf[FileSplit]
      it.map(x=>sp.getPath.getName+"|"+x)
    })

//    washData.write.parquet("/test/outputs/bswf/20170829/13")

    washData.show(false)

    spark.stop()
  }
}
