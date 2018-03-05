package com.lhs.spark.ml.clz

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.sql.SparkSession

/**
  * 二分类
  */
object ClassesByRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("classes").getOrCreate()
    val sc = spark.sparkContext
    //开始四列分别包含 URL 、页面的 ID 、原始的文本内容和分配给页面的类别。接下来 22 列包含各种各样的数值或者类属特征。
    //最后一列为目标值， -1 为长久， 0 为短暂。
    val rawData = sc.textFile("F:\\tmpdata\\test\\ml\\Classification\\train.tsv")
    val records = rawData.map(_.split("\t"))
    //records.first().foreach(println)
    //由于数据格式的问题，我们做一些数据清理的工作，在处理过程中把额外的（ " ）去掉。
    //数据集中还有一些用 "?" 代替的缺失数据，本例中，我们直接用 0 替换那些缺失数据：
    val data = records.map{r=>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed.last.toInt
      //slice(from,until) 取Array的一部分数据
      val features = trimmed.slice(4,r.size-1).map{d=>if(d=="?") 0.0 else d.toDouble}
      LabeledPoint(label,Vectors.dense(features))
    }
    println(data.first())
    /**
      * 朴素贝叶斯模型要求特征值非负，否则碰到负的特征值程序会抛出错误。因此，需要为朴素贝叶斯模型构
      建一份输入特征向量的数据，将负特征值设为 0
      */

    val nbData = records.map{r=>
      val trimmed = r.map(_.replaceAll("\"",""))
      val label = trimmed.last.toInt
      //slice(from,until) 取Array的一部分数据
      val features = trimmed.slice(4,r.size-1).map{d=>if(d=="?") 0.0 else d.toDouble}.map(d=>if(d<0) 0.0 else d)
      LabeledPoint(label,Vectors.dense(features))
    }


    //模型训练
    val numIterations = 10
    val maxTreeDepth = 5
    //逻辑回归
    val lrModel = LogisticRegressionWithSGD.train(data,numIterations)
    //SVM
    val svmModel = SVMWithSGD.train(data,numIterations)
    //朴素贝叶斯
    val nbModel = NaiveBayes.train(nbData)
    //决策树，在决策树中，我们设置模式或者 Algo 时使用了 Entropy 不纯度估计。
    val dtModel = DecisionTree.train(data,Algo.Classification,Entropy,maxTreeDepth)

    //预测
    val dataPoint = data.first()
    println("true label : "+dataPoint.label)
    //逻辑回归的预测
    val prediction4LR = lrModel.predict(dataPoint.features)
    println("lr prediction : "+prediction4LR)

    //SVM的预测
    val prediction4SVM = svmModel.predict(dataPoint.features)
    println("SVM prediction : "+prediction4SVM)

    //朴素贝叶斯的预测
    val prediction4NB = nbModel.predict(dataPoint.features)
    println("NaiveBayes prediction : "+prediction4NB)

    //决策树
    val prediction4DT = dtModel.predict(dataPoint.features)
    println("DecisionTree prediction : "+prediction4DT)

    //评估分类模型的性能
    //通常在二分类中使用的评估方法包括：预测正确率和错误率、准确率和召回率、准确率--召回率曲线下方的面积、 ROC 曲线、 ROC 曲线下的面积和 F-Measure 。

    //1：预测的正确率和错误率
    val dataCount = data.count()
    val  lrTotalCorrect = data.map(point=>if(lrModel.predict(point.features)==point.label) 1.0 else 0.0).sum()
    println("lr correction "+(lrTotalCorrect/dataCount))

    val  svmTotalCorrect = data.map(point=>if(svmModel.predict(point.features)==point.label) 1.0 else 0.0).sum()
    println("svm correction "+(svmTotalCorrect/dataCount))

    val  nbTotalCorrect = data.map(point=>if(nbModel.predict(point.features)==point.label) 1.0 else 0.0).sum()
    println("NaiveBayes correction "+(nbTotalCorrect/dataCount))

    val  dtTotalCorrect = data.map(point=>if(lrModel.predict(point.features)==point.label) 1.0 else 0.0).sum()
    println("DecisionTree correction "+(dtTotalCorrect/dataCount))

    //2：二分类的 PR 和 ROC 曲线下的面积
    val metrics = Seq(lrModel,svmModel).map(model=>{
      val scoreAndLabels = data.map(point=>{
        (model.predict(point.features),point.label)
      })
      val metric = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName,metric.areaUnderPR(),metric.areaUnderROC())
    })

    val nbMetrics = Seq(nbModel).map(model=>{
      val scoreAndLabels = nbData.map(point=>{
        val score = model.predict(point.features)
        (if(score>0.5) 1.0 else 0.0,point.label)
      })
      val metric = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName,metric.areaUnderPR(),metric.areaUnderROC())
    })

    val dtMetrics = Seq(dtModel).map{ model =>
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }

    val allMetrics = metrics ++ nbMetrics ++ dtMetrics
    allMetrics.foreach{ case (m, pr, roc) =>
      println(f"$m, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100.0}%2.4f%%")
    }

    //改进模型性能以及参数调优
    //5.6.1特征标准化
    //我们使用的许多模型对输入数据的分布和规模有着一些固有的假设，其中最常见的假设形式是特征满足正态分布。

    // RowMatrix 是一个由向量组成的 RDD ，其中每个向量是分布矩阵的一行。
    val vectors = data.map(lp => lp.features)

    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()
    //输出每列的统计值
    println("每列的均值 : "+matrixSummary.mean)
    println("每列的最小值 : "+matrixSummary.min)
    println("每列的最大值 : "+matrixSummary.max)
    println("每列的方差 : "+matrixSummary.variance)
    println("每列的非零数量 : "+matrixSummary.numNonzeros)

    //因为我们的数据在原始形式下，确切地说并不符合标准的高斯分布。为使数据更符合模型的假设，可以对每个特征进行标准化，使得每个特征是 0 均值和单位标准差。
    val scaler = new StandardScaler(withMean = true,withStd = true).fit(vectors)
    val scaledData = data.map((lp=>LabeledPoint(lp.label,scaler.transform(lp.features))))
    println("标准化之前："+data.first.features)
    println("标准化之后："+scaledData.first.features)


    //现在我们使用标准化的数据重新训练模型。这里只训练逻辑回归（因为决策树和朴素贝叶斯不受特征标准话的影响）
    val lrModelScaled = LogisticRegressionWithSGD.train(scaledData,numIterations)
    val lrTotalCorrentScaled = scaledData.map(point=>{
      if(lrModelScaled.predict(point.features)==point.label) 1 else 0
    }).sum()

    val lrAccuracyScaled = lrTotalCorrentScaled / dataCount
    val lrPredictionsVsTrue = scaledData.map(point=>{
      (lrModelScaled.predict(point.features),point.label)
    })

    val lrMetricsScaled = new BinaryClassificationMetrics(lrPredictionsVsTrue)
    val lrPr = lrMetricsScaled.areaUnderPR()
    val lrRoc = lrMetricsScaled.areaUnderROC()

    println(f"标准化之后 ：${lrModelScaled.getClass.getSimpleName}\tAccuracy:${lrAccuracyScaled * 100}%2.4f%%\tArea under PR: ${lrPr *100.0}%2.4f%%\tArea under ROC: ${lrRoc * 100.0}%2.4f%%")

    //5.6.2 其他的特征：
    //在这里我们添加一下类别（category）对性能的影响
    val categories = records.map(r=>r(3)).distinct().collect().zipWithIndex.toMap
    val numCategories = categories.size
    println("类别的数目："+numCategories+"\t类别："+categories)

    //类别的数目是14，我们需要建立一个长度为14 的向量来表达类别特征，然后根据每个样本所属类别的索引，对应的维度赋值为1，其他的为0
    val dataCategories = records.map(r=>{
      val trimed = r.map(_.replaceAll("\"",""))
      val label = trimed.last.toInt
      val categoryID = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryID) = 1.0
      val otherFeatures = trimed.slice(4,r.size-1).map(d=>if(d=="?") 0.0 else d.toDouble)
      val features = categoryFeatures ++ otherFeatures
      LabeledPoint(label,Vectors.dense(features))
    })
    //其中第一部分是一个 14 维的向量，向量中类别对应索引那一维为 1 。
    println("标准化之前："+dataCategories.first())

    //标准化

    val scalerWithCategory = new StandardScaler(withMean = true,withStd = true).fit(dataCategories.map(x=>x.features))
    val scaledDataWithCategory = dataCategories.map(lp=>LabeledPoint(lp.label,scalerWithCategory.transform(lp.features)))
    //标准化之后的特征,虽然原始特征是稀疏的（大部分维度是 0 ），但对每个项减去均值之后，将得到一个非稀疏（稠密）的特征向量表示
    println("标准化之后："+scaledDataWithCategory.first())

    val lrModelScaledWithCategory = LogisticRegressionWithSGD.train(scaledDataWithCategory,numIterations)
    val lrTotalCorrentScaledWithCategory = scaledDataWithCategory.map(point=> if(lrModelScaledWithCategory.predict(point.features)==point.label) 1 else 0).sum()
    val lrAccuracyScaedWithCategory = lrTotalCorrentScaledWithCategory / dataCount
    val lrPredictionVSTrueWithCategory = scaledDataWithCategory.map(point=>(lrModelScaledWithCategory.predict(point.features),point.label))
    val lrMetricsScaledWithCategory = new BinaryClassificationMetrics(lrPredictionVSTrueWithCategory)
    val lrPrWithCategory = lrMetricsScaledWithCategory.areaUnderPR()
    val lrRocWithCategory = lrMetricsScaledWithCategory.areaUnderROC()
    //竞赛中性能最好模型的 AUC 为 0.889 06
    println(f"加上类别之后：${lrModelScaledWithCategory.getClass.getSimpleName}\tAccuracy:${lrAccuracyScaedWithCategory * 100}%2.4f%%\tArea under PR: ${lrPrWithCategory *100.0}%2.4f%%\tArea under ROC: ${lrRocWithCategory * 100.0}%2.4f%%")

    //5.6.3 使用正确的数据格式： 对于贝叶斯模型而言  我开始时使用的数值特征并不符合假定的输入分布，所以模型性能不好也并不是意料之外。
    val dataNB = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0
      LabeledPoint(label, Vectors.dense(categoryFeatures))
    }

    val nbModelCats = NaiveBayes.train(dataNB)
    val nbTotalCorrectCats = dataNB.map { point =>
      if (nbModelCats.predict(point.features) == point.label) 1 else 0
    }.sum
    val nbAccuracyCats = nbTotalCorrectCats / dataCount
    val nbPredictionsVsTrueCats = dataNB.map { point =>
      (nbModelCats.predict(point.features), point.label)
    }
    val nbMetricsCats = new BinaryClassificationMetrics(nbPredictionsVsTrueCats)
    val nbPrCats = nbMetricsCats.areaUnderPR
    val nbRocCats = nbMetricsCats.areaUnderROC
    println(f"加上类别之后：${nbModelCats.getClass.getSimpleName}\tAccuracy: ${nbAccuracyCats * 100}%2.4f%%\tArea under PR: ${nbPrCats * 100.0}%2.4f%%\tArea under ROC: ${nbRocCats * 100.0}%2.4f%%")

    spark.close()
  }
}
