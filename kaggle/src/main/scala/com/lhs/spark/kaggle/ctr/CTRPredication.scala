package com.lhs.spark.kaggle.ctr

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 描述
  *
  * @author lihuasong
  *
  *         2019-03-12 09:22
  **/
object CTRPredication {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("error")
    val data = spark.read.option("header","true").csv("F:\\tmpdata\\kaggle\\ctr\\train")

    val splited = data.randomSplit(Array(0.7,0.3),2L)

    val catalogFeatures = Array("click","site_id","site_domain","site_category","app_id","app_domain","app_category","device_id","device_ip","device_model")
    var train_index = splited(0)
    var test_index = splited(1)

    for(catalog_feature <- catalogFeatures){
      val indexer = new StringIndexer()
        .setInputCol(catalog_feature)
        .setOutputCol(catalog_feature.concat("_index"))
      val train_index_model = indexer.fit(train_index)
      val train_indexed = train_index_model.transform(train_index)
      val test_indexed = indexer.fit(test_index).transform(test_index,train_index_model.extractParamMap())
      train_index = train_indexed
      test_index = test_indexed
    }
//    println("字符串编码下标标签：")
//    train_index.show(5,false)
//    test_index.show(5,false)

    //    特征Hasher
//    val hasher = new
//      .setInputCols("site_id_index","site_domain_index","site_category_index","app_id_index","app_domain_index","app_category_index","device_id_index","device_ip_index","device_model_index","device_type","device_conn_type","C14","C15","C16","C17","C18","C19","C20","C21")
//      .setOutputCol("feature")

    val assembler = new VectorAssembler()
      .setInputCols(Array("site_id_index","site_domain_index","site_category_index","app_id_index","app_domain_index","app_category_index","device_id_index","device_ip_index","device_model_index","device_type","device_conn_type","C14","C15","C16","C17","C18","C19","C20","C21"))
      .setOutputCol("features")

    val trainVA = assembler.transform(train_index)
    val testVA = assembler.transform(test_index)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0)
      .setFeaturesCol("feature")
      .setLabelCol("click_index")
      .setPredictionCol("click_predict")

    val model = lr.fit(trainVA)
    println(s"每个特征对应系数: ${model.coefficients} 截距: ${model.intercept}")
    model.save("CRT")

    val predications = model.transform(testVA)

    predications.select("click_index","click_predict","probability").show(100,false)
    val predictionRdd = predications.select("click_predict","click_index").rdd.map{
      case Row(click_predict:Double,click_index:Double)=>(click_predict,click_index)
    }

//    val metrics = new MulticlassMetrics(predictionRdd)
//    val accuracy = metrics.accuracy
//    val weightedPrecision = metrics.weightedPrecision
//    val weightedRecall = metrics.weightedRecall
//    val f1 = metrics.weightedFMeasure
//    println(s"LR评估结果：\n分类正确率：${accuracy}\n加权正确率：${weightedPrecision}\n加权召回率：${weightedRecall}\nF1值：${f1}")

    val evaluator = new MulticlassClassificationEvaluator()

    spark.stop()

  }

}
