package com.lhs.spark.ml.etl.transform

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.StringIndexer

/**
  * StringIndexer转换器可以把一列类别型的特征（或标签）进行编码，
  * 使其数值化，索引的范围从0开始，
  * 该过程可以使得相应的特征索引化，
  * 使得某些无法接受类别型特征的算法可以使用，
  * 并提高诸如决策树等机器学习算法的效率。
  *
  * 索引构建的顺序为标签的频率，优先编码频率较大的标签，所以出现频率最高的标签为0号。
  * 如果输入的是数值型的，我们会把它转化成字符型，然后再对其进行编码
  *
  * @author lihuasong
  *
  *         2019-03-12 13:03
  **/
object StringIndexer {
  def main(args: Array[String]): Unit = {
    val spark = MLUtils.getSpark

    val df = spark.createDataFrame(
      Seq(
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c"))
    ).toDF("id", "category")

    //随后，我们创建一个StringIndexer对象，设定输入输出列名，
    // 其余参数采用默认值，
    // 并对这个DataFrame进行训练，
    // 产生StringIndexerModel对象,
    // 最后利用该对象对DataFrame进行转换操作。
    // 可以看到，StringIndexerModel依次按照出现频率的高低，把字符标签进行了排序，
    // 即出现最多的“a”被编号成0，“c”为1，出现最少的“b”为2。

    val stringIndexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("index")

    val indexed = stringIndexer.fit(df)
    val model = indexed.transform(df)
    model.show(false)

    // 考虑这样一种情况，
    // 我们使用已有的数据构建了一个StringIndexerModel，
    // 然后再构建一个新的DataFrame，
    // 这个DataFrame中有着模型内未曾出现的标签“d”，
    // 用已有的模型去转换这一DataFrame会有什么效果？
    // 实际上，如果直接转换的话，Spark会抛出异常，报出“Unseen label: d”的错误。
    val df2 = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"), (6, "d"))
    ).toDF("id", "category")
    indexed.setHandleInvalid("skip").transform(df2).show(false)
  }
}
