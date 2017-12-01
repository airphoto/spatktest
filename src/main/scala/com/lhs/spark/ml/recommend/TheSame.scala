package com.lhs.spark.ml.recommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object TheSame {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("RecommendByALS").getOrCreate()
    val sc = spark.sparkContext
    val rawData = sc.textFile("F:\\tmpdata\\test\\ml\\ml-100k\\ml-100k\\u.data")
    println(rawData.first())
    // 字段顺序 用户ID、影片ID、星级和时间戳
    val rawRatings = rawData.map(_.split("\t").take(3))

    //ALS 模型需要一个有Rating记录构成的RDD ，Rating就是对user，product和评分的封装
    val ratings = rawRatings.map{case Array(user,movie,rating)=> Rating(user.toInt,movie.toInt,rating.toDouble)}

    //训练推荐模型
    /**
      *   rank ：对应ALS模型中的因子个数，也就是在低阶近似矩阵中的隐含特征个数。因子个
      * 数一般越多越好。但它也会直接影响模型训练和保存时所需的内存开销，尤其是在用户
      * 和物品很多的时候。因此实践中该参数常作为训练效果与系统开销之间的调节参数。通
      * 常，其合理取值为10到200。
      *   iterations ：对应运行时的迭代次数。ALS能确保每次迭代都能降低评级矩阵的重建误
      * 差，但一般经少数次迭代后ALS模型便已能收敛为一个比较合理的好模型。这样，大部分
      * 情况下都没必要迭代太多次（10次左右一般就挺好）。
      *   lambda ：该参数控制模型的正则化过程，从而控制模型的过拟合情况。其值越高，正则
      * 化越严厉。该参数的赋值与实际数据的大小、特征和稀疏程度有关。和其他的机器学习
      * 模型一样，正则参数应该通过用非样本的测试数据进行交叉验证来调整。
      */
    val model: MatrixFactorizationModel  = ALS.train(ratings,50,10,0.01)

    val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))

    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    cosineSimilarity(itemVector, itemVector)

    //求余弦相似度
    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }


    val movies = sc.textFile("F:\\tmpdata\\test\\ml\\ml-100k\\ml-100k\\u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()
    //取出与物品567最相似的前10个物品  top要比collect好啊
    val sortedSims = sims.top(11)(Ordering.by[(Int, Double), Double] { case(id, similarity) => similarity })
    sortedSims.slice(1,11).map{case (id,similarity)=>(titles(id),similarity)}.foreach(println)

    //推荐模型效果的评估

    // 均方差（MSE）
    val moviesForUser = model.predict()
    val actualRating = moviesForUser.take(1)(0)

  }

  //余弦相似度是两个向量在n维空间里两者夹角的度数。它是两个向量的点积 与 各向量范数（或长度）的乘积 的商。 余弦相似度是一个正则化了的点积。
  //该相似度的取值在-1到1之间。1表示完全相似，0表示两者互不相关（即无相似性）。
  // 这种衡量方法很有帮助，因为它还能捕捉负相关性。
  // 也就是说，当为-1时则不仅表示两者不相关，还表示它们完全不同。
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
