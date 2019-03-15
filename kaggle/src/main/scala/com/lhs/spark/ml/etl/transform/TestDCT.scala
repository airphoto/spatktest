package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.{DCT, VectorAssembler}

/**
  * 描述
  *
  * Discrete Cosine Transform（离散余弦变换）
  * 是将时域的N维实数序列转换成频域的N维实数序列的过程（有点类似离散傅里叶变换）。
  *
  * （ML中的）DCT类提供了离散余弦变换DCT-II的功能，
  * 将离散余弦变换后结果乘以 得到一个与时域矩阵长度一致的矩阵。
  *
  * 没有偏移被应用于变换的序列（例如，变换的序列的第0个元素是第0个DCT系数，而不是第N / 2个），即输入序列与输出之间是一一对应的。
  *
  * @author lihuasong
  *
  *         2019-03-15 12:55
  **/
object TestDCT {
  def main(args: Array[String]): Unit = {

    val spark = MLUtils.getSpark
    import spark.implicits._

    val data = spark.createDataset(Seq(
      (0.0, 1.0, -2.0, 3.0),
      (-1.0, 2.0, 4.0, -7.0),
      (14.0, -2.0, -5.0, 1.0))
    ).toDF("c1","c2","c3","c4")

    val assember = new VectorAssembler()
      .setInputCols(Array("c1","c2","c3","c4"))
      .setOutputCol("features")

    val df = assember.transform(data)

    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("dct_features")
      .setInverse(false)

    dct.transform(df).show(false)
  }
}
