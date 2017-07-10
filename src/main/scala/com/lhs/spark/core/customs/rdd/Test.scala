package com.lhs.spark.core.customs.rdd

import com.lhs.spark.core.customs.add_functions.SalesRecord
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <P>@ObjectName : Test</P>
  *
  * <P>@USER : abel.li </P>
  *
  * <P>@CREATE_AT : 2016/6/22 </P>
  *
  * <P>@DESCRIPTION : TODO </P>
  */

object Test {
   def main(args: Array[String]) {
     val sparkConf = new SparkConf().setMaster("local[*]").setAppName("add_functions")
     val sc = new SparkContext(sparkConf)
     val df = sc.parallelize(Array("1,1,1,100.0"))
     val salesRecordRDD = df.map(x=>{
       val fields = x.split(",")
       new SalesRecord(fields(0),fields(1),fields(2),fields(3).toDouble)
     })

     val disCountRDD = new IteblogDiscountRDD(salesRecordRDD,0.1)
     disCountRDD.collect().foreach(x=>println(x.itemValue))
     sc.stop()
   }
 }
