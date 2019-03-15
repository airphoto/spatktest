package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.Binarizer

/**
  * 连续特征根据阈值二值化，大于阈值的为1.0，小于等于阈值的为0.0。二值化是机器学习中很常见的思路，可以将连续型数据转化为离散型。
  *
  * @author lihuasong
  *
  *         2019-03-12 13:00
  **/
object Binarizer {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark
    import spark.implicits._

    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    val binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataframe = binarizer.transform(dataFrame)
    binarizedDataframe.show(false)

  }
}
