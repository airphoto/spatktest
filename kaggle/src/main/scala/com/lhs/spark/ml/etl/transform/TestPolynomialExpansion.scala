package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors

/**
  * 描述
  * Polynomial expansion
  * （多项式展开）是将特征扩展为多项式空间的过程，
  * 多项式空间由原始维度的n度组合组成。
  * PolynomialExpansion类提供此功能。
  *
  * 下面的例子显示了如何将您的功能扩展到3度多项式空间。
 *
  * @author lihuasong
  *
  *         2019-03-15 12:41
  **/
object TestPolynomialExpansion {
  def main(args: Array[String]): Unit = {

    val spark = MLUtils.getSpark

    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polynomial_features")
      .setDegree(2)

    polynomialExpansion.transform(df).show(false)

  }
}
