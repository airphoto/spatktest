package com.lhs.spark.ml.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object KmeansExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    import spark.implicits._

    //标记,长度，位置，数据,这个应该是  隐式矩阵
//    val data = spark.read.format("libsvm").load("E:\\sources\\spark-2.2.0\\data\\mllib\\sample_kmeans_data.txt")
    val data = spark.read.textFile("F:\\tmpdata\\test\\ml\\read")
    val transData = data.map(line=>{
      val field = line.split(" ")
      val label = field(0).toDouble
      val fs = field.slice(1,field.length).map(_.toDouble)
      LabeledPoint(label,Vectors.dense(fs))
    })
    transData.show(false)
    transData.printSchema()
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(transData)

    val WSSSE = model.computeCost(transData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // 显示结果
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    model.transform(transData).show(false)
    spark.close()
  }
}
