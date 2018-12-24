package com.lhs

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by Administrator on 2017/1/13.
 */

/**
 * <P>@ObjectName : ImplicitDefDemo</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/1/13 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

case class DimDate(date_dim_id:Int,year:Int,quarter:Int,month:Int,week:Int,day:Int,day_of_week:Int,day_of_year:Int,day_caption:String,day_caption_short:String,month_caption:String,week_caption:String,quarter_caption:String)

object ImplicitDefDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  object MyImplicitTypeConversion {
    implicit def str2Int(str: String) = str.toInt
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").appName("app").getOrCreate()
    import  spark.implicits._
    val calendar = Calendar.getInstance()
    calendar.set(2016,0,1)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val format2 = new SimpleDateFormat("yyyyMMdd")
    val dimDate = (1 to 3000).map(i=>{
      val year = calendar.get(Calendar.YEAR)
      val dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH)
      val dayOFYear = calendar.get(Calendar.DAY_OF_YEAR)
      val month = calendar.get(Calendar.MONTH) + 1
      val quarter = ((month-1)/3)+1
      val week = calendar.get(Calendar.WEEK_OF_YEAR)
      val dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK)
      val dayCaption = format.format(calendar.getTime)
      val dayCaptionShort = format2.format(calendar.getTime)
      val monthCaption = if(month<10) s"$year-0$month" else s"$year-$month"
      val weekCaption = if(week<10) s"$year-0$week" else s"$year-$week"
      val quarterCaption = s"$year-0$quarter"
      calendar.add(Calendar.DAY_OF_YEAR,1)
      DimDate(i,year,quarter,month,week,dayOfMonth,dayOfWeek,dayOFYear,dayCaption,dayCaptionShort,monthCaption,weekCaption,quarterCaption)
    }).toList
    val ds = spark.createDataset(dimDate).toDF()
//    val ds = spark.createDataFrame(dimDate).toDF("date_dim_id","year","quarter","month","week","day","day_of_week","day_of_year","day_caption","month_caption","week_caption")
//    val proper = new Properties()
//    proper.put("user","root")
//    proper.put("password","123456")
//    ds.repartition(10).write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/test","dim_date",proper)
    ds.coalesce(1).write.parquet("date_parquet")
    ds.show(false)
    spark.close()
    val data = Traversable(1,2,3)

    val map = Map.apply(""->"",""->"",""->"")
  }
}
