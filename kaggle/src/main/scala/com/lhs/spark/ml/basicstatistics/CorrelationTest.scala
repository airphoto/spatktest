package com.lhs.spark.ml.basicstatistics

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

/**
  * 计算两组数据之间的相关性是统计中常见的操作。
  *
  * 在spark中我们提供了计算多个系列之间两两相关关系的灵活性。
  *
  * 目前支持的相关方法是Pearson’s（皮尔逊相关系数）和Spearman’s correlation（斯皮尔曼相关系数）。
  *
  * @author lihuasong
  *
  *         2019-03-12 13:54
  **/
object CorrelationTest {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark
    import spark.implicits._
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = data.map(Tuple1.apply(_)).toDF("features")
    val Row(coeff1:Matrix) = Correlation.corr(df,"features").head()
    println("皮尔逊相关系数 ：")
    println(coeff1.toString())


    val Row(coeff2:Matrix) = Correlation.corr(df,"features","spearman").head()
    println("斯皮尔曼相关系数：")
    println(coeff2.toString())
    spark.stop()
  }
}
