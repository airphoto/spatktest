package com.lhs.spark.ml.basicstatistics

import com.lhs.spark.ml.MLUtils

/**
  * 假设检验是统计学中一个强有力的工具，
  *
  * 用来确定一个结果是否具有统计学意义，这个结果是否偶然发生。
  *
  * spark 目前支持皮尔逊卡方(χ2)为独立测试。
  *
  * ChiSquareTest对每一项针对标签的功能都进行皮尔逊独立测试。
  *
  * 对于每个特征，将(特征、标签)对转换为一个列联矩阵，计算卡方统计量。
  * 所有标签和特性值必须是分类的。
  *
  * @author lihuasong
  *
  *         2019-03-12 14:03
  **/
object HypothesisTest {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark
    import spark.implicits._

    import org.apache.spark.ml.linalg.{Vector, Vectors}
    import org.apache.spark.ml.stat.ChiSquareTest

    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    val df = data.toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println("pValues = " + chi.getAs[Vector](0))
    println("degreesOfFreedom = " + chi.getSeq[Int](1).mkString("[", ",", "]"))
    println("statistics = " + chi.getAs[Vector](2))

  }
}
