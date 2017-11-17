package com.lhs.work

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Created by Administrator on 2017/6/28.
 */

/**
 * <P>@ObjectName : JiangSu</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/6/28 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object JiangSu {
//  main
def getTimeDaysLater(baseTime:String,days:Int)={
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar = Calendar.getInstance()
  calendar.setTime(format.parse(baseTime))
  calendar.set(Calendar.DATE,calendar.get(Calendar.DATE)+days)
  format.format(calendar.getTime)
}

  def daysAfter(dabiao:String,baseDay:String)={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dabiaoTime = format.parse(dabiao)
    val baseDayTime = format.parse(baseDay)

  }

  def daysOfTwo(dabiao:String, detail:String) = {
    val dabiaoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val detailFormat = new SimpleDateFormat("yyyy/MM/dd")
    val aCalendar = Calendar.getInstance();
    aCalendar.setTime(dabiaoFormat.parse(dabiao));
    val day1 = aCalendar.get(Calendar.DAY_OF_YEAR);
    aCalendar.setTime(detailFormat.parse(detail));
    val day2 = aCalendar.get(Calendar.DAY_OF_YEAR);
    day2 - day1
  }

  def maxOfTwo(tmp1:String, tmp2:String) = {
    val format = new SimpleDateFormat("yyyy/MM/dd")
    val d1 = format.parse(tmp1)
    val d2 = format.parse(tmp2)
    d1.getTime - d2.getTime
  }

  def main(args: Array[String]): Unit = {
//    println(getTimeDaysLater("2017-08-17 11:57:30",5))
//    println(daysOfTwo("2017-08-17 11:57:30","2017/8/16"))
    println(maxOfTwo("2017/8/17","2017/8/16"))
  }
}
