package com.lhs.spark.core.session

import org.apache.spark.sql.SparkSession

/**
 * Created by Administrator on 2017/6/16.
 */

/**
 * <P>@ObjectName : CallingSort</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/6/16 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object CallingSort {
  def main(args: Array[String]) {
    val spark = SparkSession
                        .builder()
                        .master("local[*]")
                        .appName("CallingSort")
                        .getOrCreate()
    val df = spark.sparkContext.textFile("/user/callLog/stat_date=20170320")
    val callings = df.map(x=>(x.split("\\|")(9),1))
    val data = callings.reduceByKey(_+_).sortBy(_._2,false)
    data.map(x=>x._1+"\t"+x._2).collect().foreach(println)
  }
}
