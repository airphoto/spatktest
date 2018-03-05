package com.lhs.spark.sql

import java.util.Properties

import com.lhs.spark.dataset.Messages.GamePlay
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
                      .builder()
                      .master("local[*]")
                      .appName("sqlTest")
                      .getOrCreate()

    import spark.implicits._

    val df = spark.read.textFile("F:\\tmpdata\\test\\common\\first")

    //    df.map(x=>x.split("\\|").length).collect().foreach(println)

    val washData = df.filter(_.split("\\|").length == 19).map(x=>{
      val Array(tag, endTime, gameID, playID, options, playTimes, factTimes, dismiss, userIDs, mIP, mPort, mID, startTime, tableID, gameType, roundBase, cards, factCards, version) = x.replace("[", "").replace("]", "").split("\\|")
      GamePlay(tag, endTime, gameID, playID, options.split(";").filter(_.nonEmpty), playTimes, factTimes, dismiss, userIDs.split(";").filter(_.nonEmpty), mIP, mPort, mID, startTime, tableID, gameType, roundBase, cards, factCards, version)
    })

    val selectData = washData.select('gameID,'options)
    selectData.show(false)
    selectData.select($"gameID",size($"options")).show(false)

//    val selectData = washData.select('gameID,explode('options).as("option"),'cards,'factCards,'playTimes,'factTimes)

//    selectData.groupBy("gameID").agg(collect_set($"option").as("options")).map(r=>{
//      val ops = r.getAs[mutable.WrappedArray[String]]("options")
//    }).show(false)
//    val countData = selectData.groupBy('gameID,'option).agg(sum('cards).as('sum_cards),sum('factCards).as('sum_fact_cards),sum('playTimes).as('sum_plays),sum('factTimes).as('sum_fact_plays))

    spark.close()
  }
}
