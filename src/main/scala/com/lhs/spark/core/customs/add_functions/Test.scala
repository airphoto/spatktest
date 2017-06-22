package com.lhs.spark.core.customs.add_functions

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2017/6/22.
 */

/**
 * <P>@ObjectName : Test</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/6/22 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object Test {
  def main(args: Array[String]) {
    import IteblogCustomFunctions._
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("add_functions")
    val sc = new SparkContext(sparkConf)
    val df = sc.textFile("")
    val salesRecordRDD = df.map(x=>{
      val fields = x.split(",")
      new SalesRecord(fields(0),fields(1),fields(2),fields(3).toDouble)
    })

    salesRecordRDD.coalesce(10)

    sc.stop()
  }
}
