package com.lhs.spark.core.transformations

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
    val sc = new SparkContext(sparkConf)
    val df = sc.textFile("F:\\tmpdata\\test\\resources\\people.txt")

    val zero = ArrayBuffer[Int]()
    val addToArray = (buffer:ArrayBuffer[Int],v:Int)=> buffer += v
    val combine = (buffer1:ArrayBuffer[Int],buffer2:ArrayBuffer[Int]) => buffer1 ++= buffer2

    val pairData = df.mapPartitions(it=>{
      it.map(x=>{
        val fields = x.split(",")
        val name = fields(0)
        val age = fields(1).trim.toInt
        (name,age)
      })
    })

    val aggreData = pairData.aggregateByKey(zero)(addToArray,combine)

    aggreData.collect().foreach(println)
    sc.stop()
  }
}
