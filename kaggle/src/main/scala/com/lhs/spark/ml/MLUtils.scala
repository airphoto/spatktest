package com.lhs.spark.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 描述
  *
  * @author lihuasong
  *
  *         2019-03-12 11:58
  **/
object MLUtils {

  def getSpark:SparkSession = {
    SparkSession.builder()
      .master("local[*]")
//      .config("spark.eventLog.enabled","false")
      .config("spark.logConf","true")
      .getOrCreate()

  }

}
