package com.lhs.scala.collections

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ArrayTest {
  def main(args: Array[String]): Unit = {
//    val array1 = Array("1","2")
//    val array2 = Array("3","4")
//    val buffer = new ArrayBuffer[String]()
//
//    buffer ++= array1 ++= array2
//    val index = buffer.indexOf("2")
//    buffer.remove(index)
//    buffer.insert(index,"10")
//    buffer -= "4"
//    buffer.foreach(println)

    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val rdd1 = spark.createDataset(Seq(("u1",("A","B","20180910154040")),("u1",("B","C","20180910154041")),("u1",("C","D","20180910154042")),("u1",("D","E","20180910154043")),("u1",("E","F","20180910154044")))).rdd
    rdd1.groupByKey().map{case (key,it)=>{
      val sortedData = it.toArray.sortWith((a,b)=>a._3<b._3)
      val ll2 = sortedData.zipWithIndex.map(p=>{
        p._2 match {
          case 0 => s"${sortedData(0)._1}->${sortedData(0)._2}"
          case _ => s"${sortedData(0)._1}->${sortedData(0)._2}->${sortedData.slice(1,p._2+1).map(_._2).reduceLeft((p1,p2)=>s"${p1}->${p2}")}"
        }
      })
      (key,ll2)
    }}.flatMap{case (k,v)=>v.map(x=>(k,x))}.collect().foreach(println)
  }
}
