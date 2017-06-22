package com.lhs.spark

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

/**
 * Created by Administrator on 2016/12/27.
 */

/**
 * <P>@ObjectName : Test</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2016/12/27 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object Test {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)

    sc.stop()
  }
}
