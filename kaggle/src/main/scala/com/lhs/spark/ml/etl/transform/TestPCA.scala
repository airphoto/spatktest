package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors

/**
  * 描述
  *
  * PCA 是使用正交变换将可能相关变量的一组观察值转换为称为主成分的线性不相关变量的值的一组统计过程。
  *
  * PCA 类训练使用 PCA 将向量投影到低维空间的模型。
  *
  * 下面的例子显示了如何将5维特征向量投影到3维主成分中。
  *
  * @author lihuasong
  *
  *         2019-03-13 20:13
  **/
object TestPCA {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark
    import spark.implicits._

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    pca.transform(df).show(false)
  }
}
