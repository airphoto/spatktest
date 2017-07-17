package com.lhs.spark.sql

import java.util.Properties

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2017/7/14.
 */

/**
 * <P>@ObjectName : SQLTest</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/7/14 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object SQLTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession
                      .builder()
                      .master("local[*]")
                      .appName("sqlTest")
                      .getOrCreate()

    sparkSession.read.csv("")

    sparkSession.close()
  }
}
