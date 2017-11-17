package com.lhs.spark.dataset

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object DataCreater {
  def main(args: Array[String]): Unit = {

    val writer = new PrintWriter(new File("match6.txt"))
    (1 to 100000).foreach(i=> {
      val uuid = UUID.randomUUID().toString
      val typeRandom = Random.nextInt(8)
      val gameRandom = s"210${Random.nextInt(50)}"
      val calendar = Calendar.getInstance()
      calendar.add(Calendar.DAY_OF_MONTH,-1)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")

      val oneFullSite = (0 to 3).map(index => {
        if (index == 0) {
          val time = format.format(calendar.getTime)
          s"match|site|0|[${time}]|${gameRandom}|${typeRandom}|${uuid}|0|16|16|0|V5.00.00"
        }
        else if (index == 3) {
          calendar.add(Calendar.SECOND,index*20)
          val time = format.format(calendar.getTime)
          s"match|site|2|[${time}]|${gameRandom}|${typeRandom}|${uuid}|1|0|0|V5.00.00"
        }
        else {
          calendar.add(Calendar.SECOND,index*20)
          val time = format.format(calendar.getTime)
          s"match|site|1|[${time}]|${gameRandom}|${typeRandom}|${uuid}|2|${8 / index}|0|V5.00.00"
        }
      })

      oneFullSite.foreach(writer.println(_))
      val oneFullUser = (0 to 15).map(index => {
        calendar.add(Calendar.SECOND,-index-120)
        val time = format.format(calendar.getTime)
        val userid = s"373${Random.nextInt(1000)}"
        s"match|user|0|[${time}]|${gameRandom}|${userid}|21001|${typeRandom}|${uuid}|1|4|0|V5.00.00"
      })
      oneFullUser.foreach(writer.println(_))


      val calendar2 = Calendar.getInstance()
      val timingUUID= UUID.randomUUID().toString
      val timingTypeRandom = 10+Random.nextInt(8)
      val timingGameRandom = s"210${Random.nextInt(50)}"

      val timingSite = (0 to 3).map(index => {
        if (index == 0) {
          val time = format.format(calendar2.getTime)
          s"match|site|0|[${time}]|${timingGameRandom}|${timingTypeRandom}|${timingUUID}|18|16|16|1|V5.00.00"
        }
        else if (index == 3) {
          calendar2.add(Calendar.SECOND,index*20)
          val time = format.format(calendar2.getTime)
          s"match|site|2|[${time}]|${timingGameRandom}|${timingTypeRandom}|${timingUUID}|1|0|1|V5.00.00"
        }
        else {
          calendar2.add(Calendar.SECOND,index*20)
          val time = format.format(calendar2.getTime)
          s"match|site|1|[${time}]|${timingGameRandom}|${timingTypeRandom}|${timingUUID}|2|${8 / index}|1|V5.00.00"
        }
      })

      timingSite.filter(x=>i%200==0).foreach(writer.println(_))

      val timingUser = (0 to 15).map(index => {
        calendar2.add(Calendar.SECOND,-index-120)
        val time = format.format(calendar2.getTime)
        val userid = s"173${Random.nextInt(1000)}"
        s"match|user|0|[${time}]|${timingGameRandom}|${userid}|21001|${timingTypeRandom}|${timingUUID}|1|4|1|V5.00.00"
      })

      timingUser.filter(x=>i%200==0).foreach(writer.println)

      println(i)
    })
    writer.close()
  }
}
