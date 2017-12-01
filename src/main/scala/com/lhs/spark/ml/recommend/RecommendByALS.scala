package com.lhs.spark.ml.recommend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object RecommendByALS {
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

    //Predict the rating of one user for one product. (user_id,product_id)
    //ALS模型的初始化是随机的，这可能让你看到的结果和这里不同。实际上，每次运行该模型所产生的推荐也会不同。
    val predictedRating = model.predict(789, 123)

    //向789用户推荐前10个产品
    val topKRecs = model.recommendProducts(789, 10)
    topKRecs.foreach(println)

    // 检验推荐内容
    val movies = sc.textFile("F:\\tmpdata\\test\\ml\\ml-100k\\ml-100k\\u.item")
    val titles = movies.map(line => line.split("\\|").take(2)).map(array=>(array(0).toInt, array(1))).collectAsMap()
    titles(123)

    val moviesForUser = ratings.keyBy(_.user).lookup(789)

    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product),rating.rating)).foreach(println)

    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)


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

    //取出与物品567最相似的前10个物品  top要比collect好啊
    val sortedSims = sims.top(11)(Ordering.by[(Int, Double), Double] { case(id, similarity) => similarity })
    sortedSims.slice(1,11).map{case (id,similarity)=>(titles(id),similarity)}.foreach(println)

    //现在从之前计算的 moviesForUser 这个 Ratings 集合里找出该用户的第一个评级
    val actualRating = moviesForUser.take(1)(0)
    val predictedRatings = model.predict(789, actualRating.product)
    val squaredError = math.pow(predictedRatings - actualRating.rating, 2.0)

    //对每个用户真实评级
    val usersProducts = ratings.map{ case Rating(user, product, rating)
    => (user, product)
    }
    //预测评级
    val predictions = model.predict(usersProducts).map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    //将两个评级连接
    val ratingsAndPredictions = ratings.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)

    val MSE = ratingsAndPredictions.map{
      case ((user, product), (actual, predicted)) => math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)

    spark.close()
  }

  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
