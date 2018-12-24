package com.lhs.spark.ml.kaggle

import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * 描述
  *
  * @author lihuasong
  *
  * 估计泰坦尼克幸福着的幸存率
  *         2018-11-22 18:38
  **/
object Titanic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("error")

    val train = spark.read.option("header","true").csv("E:\\idea\\git\\spatktest\\src\\main\\source\\train.csv")
    train.filter($"".getItem())
    train.show(2,false)
    train.printSchema()
    val vecDF = new VectorAssembler().setInputCols("Pclass|Age|SibSp|Parch|Fare".split("\\|")).setOutputCol("feature").transform(train)
    vecDF.show(2,false)
  }
}
