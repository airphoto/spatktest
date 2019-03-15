package com.lhs.spark.ml.etl.extract

/**
  * “词频－逆向文件频率”（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，它可以体现一个文档中词语在语料库中的重要程度。
  * 词频TF(t,d)是词语t在文档d中出现的次数。文件频率DF(t,D)是包含词语的文档的个数。
  * 如果我们只使用词频来衡量重要性，很容易过度强调在文档中经常出现，却没有太多实际信息的词语，比如“a”，“the”以及“of”。
  * 如果一个词语经常出现在语料库中，意味着它并不能很好的对文档进行区分。
  *
  * TF-IDF就是在数值化文档信息，衡量词语能提供多少信息以区分文档。定义如下：
  *
  * IDF(t,D)= log|D|+1 / DF(t,D)+1
  *
  * 此处 |D||D| 是语料库中总的文档数。公式中使用log函数，当词出现在所有文档中时，它的IDF值变为0。加1是为了避免分母为0的情况。TF-IDF 度量值表示如下：
  *
  * TFIDF(t,d,D)=TF(td)⋅IDF(t,D)
  *
  * 在Spark ML库中，TF-IDF被分成两部分：TF(HashingTF) 和 IDF
  *
  * HashingTF 是一个Transformer，在文本处理中，接收词条的集合然后把这些集合转化成固定长度的特征向量。这个算法在哈希的同时会统计各个词条的词频。
  *
  * IDF是一个Estimator，在一个数据集上应用它的fit（）方法，产生一个IDFModel。
  * 该IDFModel 接收特征向量（由HashingTF产生），然后计算每一个词在文档中出现的频次。
  * IDF会减少那些在语料库中出现频率较高的词的权重。
  *
  * Spark.mllib 中实现词频率统计使用特征hash的方式，原始特征通过hash函数，映射到一个索引值。
  * 后面只需要统计这些索引值的频率，就可以知道对应词的频率。
  * 这种方式避免设计一个全局1对1的词到索引的映射，这个映射在映射大量语料库时需要花费更长的时间。
  *
  * 但需要注意，通过hash的方式可能会映射到同一个值的情况，即不同的原始特征通过Hash映射后是同一个值。
  * 为了降低这种情况出现的概率，我们只能对特征向量升维。
  *
  * i.e., 提高hash表的桶数，默认特征维度是 2^20 = 1,048,576.
  * 在下面的代码段中，我们以一组句子开始。
  * 首先使用分解器Tokenizer把句子划分为单个词语。
  * 对每一个句子（词袋），我们使用HashingTF将句子转换为特征向量，最后使用IDF重新调整特征向量。
  * 这种转换通常可以提高使用文本特征的性能。
  *
  * 在下面的代码中，我们以一组句子为例，使用Tokenizer将每一条句子分解为单词，对每一条句子（词袋），我们使用HashingTF 将其转化为特征向量，最后使用IDF 重新调整特征向量。
  *
  * @author lihuasong
  *
  *         2019-03-12 11:51
  **/

import com.lhs.spark.ml.MLUtils
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

object TFIDF {
  def main(args: Array[String]): Unit = {

    val spark = MLUtils.getSpark
    import spark.implicits._

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    // 在得到文档集合后，即可用tokenizer对句子进行分词。
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    wordsData.show(false)
//    +-----+-----------------------------------+------------------------------------------+
//    |label|sentence                           |words                                     |
//    +-----+-----------------------------------+------------------------------------------+
//    |0.0  |Hi I heard about Spark             |[hi, i, heard, about, spark]              |
//    |0.0  |I wish Java could use case classes |[i, wish, java, could, use, case, classes]|
//    |1.0  |Logistic regression models are neat|[logistic, regression, models, are, neat] |
//    +-----+-----------------------------------+------------------------------------------+


    // 使用HashingTF的transform()方法把句子哈希成特征向量，这里设置哈希表的桶数为2000。
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)

    featurizedData.show(false)

    /*
    可以看到，分词序列被变换成一个稀疏特征向量，其中每个单词都被散列成了一个不同的索引值，特征向量在某一维度上的值即该词汇在文档中出现的次数

    +-----------------------------------------+
    |rawFeatures                              |
    +-----------------------------------------+
    |(20,[5,6,9],[2.0,1.0,2.0])               |
    |(20,[3,5,12,14,18],[2.0,2.0,1.0,1.0,1.0])|
    |(20,[5,12,14,18],[1.0,2.0,1.0,1.0])      |
    +-----------------------------------------+

    */

    // 最后，使用IDF来对单纯的词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力，
    // IDF是一个Estimator，调用fit()方法并将词频向量传入，即产生一个IDFModel。
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    // 很显然，IDFModel是一个Transformer，调用它的transform()方法，即可得到每一个单词对应的TF-IDF度量值。
    val resultData = idfModel.transform(featurizedData)

    resultData.show(false)

    /*
可以看到，特征向量已经被其在语料库中出现的总次数进行了修正，通过TF-IDF得到的特征向量，在接下来可以被应用到相关的机器学习方法中

[0.0,(20,[5,6,9],[0.0,0.6931471805599453,1.3862943611198906])]
[0.0,(20,[3,5,12,14,18],[1.3862943611198906,0.0,0.28768207245178085,0.28768207245178085,0.28768207245178085])]
[1.0,(20,[5,12,14,18],[0.0,0.5753641449035617,0.28768207245178085,0.28768207245178085])]
*/

  }
}
