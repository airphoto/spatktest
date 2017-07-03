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
    val df = sc.textFile("/user/callLog/stat_date=20170616")
    val filterData = df.filter(x=>(x.split("\\|")(24)=="0" && isMobile(x.split("\\|")(5))))
    val pairData = filterData.repartition(45).mapPartitions(it=>it.map(x=>{
      val fields = x.split("\\|")
      val calling = fields(5)
      val domain = fields(3)
      (calling+","+domain,1)
    }))

    val tmpData = pairData.reduceByKey(_+_,45).map(x=>{
      val Array(calling,domain) = x._1.split(",")
      (domain,calling,x._2)
      (SecondarySort(domain.toInt,x._2),domain+","+calling+","+x._2)
    })

    val sortData = tmpData.sortByKey(false)

    sortData.map(x=>x._2).coalesce(1).saveAsTextFile("/data/test/overseas")

    sc.stop()
  }

  def isMobile(str:String)= {
    if (str.startsWith("1") && str.length() == 11) true
    else if(str.length < 11) false
    else {
      // 截取后11位
      val mobilePhone = str.substring(str.length() - 11, str.length())
      val thisProvinceAreaCode = "024,0411,0412,0245,0244,0415,0416,0417,0418,0419,0427,0247,0421,0429"
      val mobilePhones = "134、135、136、137、138、139、147、150、151、152、157、158、159、172、178、182、183、184、187、188、130、131、132、145、155、156、171、175、176、185、186、170、133、149、153、173、177、180、181、189"
      // 是手机号码,判断被叫号码是否带区号前缀
      if (mobilePhones.indexOf(mobilePhone.substring(0, 3)) >= 0 && thisProvinceAreaCode.indexOf(str.substring(0, 3)) < 0 && thisProvinceAreaCode.indexOf(str.substring(0, 4)) < 0)
        true
      else
        false
    }
  }
}
