package com.lhs.scala

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, unix_timestamp, when}
import org.apache.spark.storage.StorageLevel

object Test2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val targetDay = "20180101"
    val seconds = 1514822400
    val dateid = 732

    //当天的转卡表
    val chargeTable = spark.read.table("datasystem.source_playercharge_byday").filter($"target_day"<=targetDay)
    val transferTable = spark.read.table("datasystem.source_agent_to_sub").filter($"target_day"<=targetDay)
    val orderTable = spark.read.table("datasystem.source_ordercard_byday").filter($"target_day"<=targetDay)

    val charge0 = chargeTable.filter($"chargetype"===1 && $"chargertype"==="A").map(r=>{
        (0,r.getAs[String]("chargeuser"),r.getAs[String]("servercode"),Math.abs(r.getAs[Int]("cardnum")),r.getAs[String]("target_day"))
      })

    val charge1 = transferTable.map(r=>{
      (1,r.getAs[String]("agent_id"),r.getAs[String]("server_code"),Math.abs(r.getAs[Int]("card_num")),r.getAs[String]("target_day"))
    })

    val charge2 = orderTable.filter($"buyway"==="G").map(r=>{
      (2,r.getAs[Int]("agentid").toString,r.getAs[String]("servercode"),Math.abs(r.getAs[Int]("cardnum")),r.getAs[String]("target_day"))
    })

    val charge = charge0.union(charge1).union(charge2).toDF("charge_type","agent_id","server_code","card_num","target_day").persist()
    val order = orderTable.filter($"playertype"==="V" && ($"buyway"==="ONPAY" || $"buyway"==="ONPAY_DO")).select($"agentid".as("agent_id"),$"servercode".as("server_code"),$"diamondcount".as("money"),$"cardnum".as("cards"),$"target_day")

    //截止到指定日期的代理全量表
    val agent = spark.read.table("datasystem.source_agent_all_byday").filter($"target_day"==="20180101").filter(unix_timestamp($"createtime")<seconds)
    //全部代理的地区表
    val area = spark.read.table("datasystem.source_agent_area_all_byday").filter($"target_day"==="20180101")
    //代理结合地区
    val agentWithArea = agent.join(area,Seq("agentid"),"left") .select($"createusertype".as("create_user_type"),$"agentid".as("agent_id"),$"servercode".as("server_code"),$"province",$"city",$"district",$"enable",$"createtime".as("create_time")).persist()
    //转卡数据结合地区等数据
    val charge7 = charge.filter($"target_day" > beforeDays(targetDay,7))
    val charge7WithAgent = charge7.join(agentWithArea,Seq("agent_id","server_code"))

    val chargeWithAgent = charge.join(agentWithArea,Seq("agent_id","server_code")).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val orderWithAgent = order.join(agentWithArea,Seq("agent_id","server_code")).persist(StorageLevel.MEMORY_AND_DISK_SER)

    chargeWithAgent.unpersist()
    orderWithAgent.unpersist()
    //代理每天转卡和购卡统计的数据
    val detailByDay = spark.read.table("datasystem.fact_agent_detail_count_by_day").filter($"date_dim_id"===dateid)
    val detailAll = spark.read.table("datasystem.fact_agent_detail_all").filter($"date_dim_id"===dateid && unix_timestamp($"create_time")<seconds)

    val ordert = spark.read.table("datasystem.fact_agent_order_count_by_day").filter($"date_dim_id"===dateid && $"agent_id"==="780439228").show(false)

    detailByDay.filter($"agent_id"==="780439228").show(false)



    //-----------------------------------------------------------
    ///总计的数据
    println("代理总数")
    agentWithArea.count //截止到指定日期 的代理总数
    //有效代理人数
    println("有效代理")
    orderWithAgent.filter($"server_code"==="sichuan_db").agg(countDistinct("agent_id")).show(false)
    //禁用代理人数
    println("禁用代理人数")
    agentWithArea.filter($"enable" === "N").count()//禁用代理总数

    val orderJoin = orderTable.filter($"target_day"<=targetDay && $"playertype"==="V" && ($"buyway"==="ONPAY" || $"buyway"==="ONPAY_DO")).select($"agentid".as("agent_id"),$"servercode".as("server_code")).join(agentWithArea,Seq("agent_id","server_code"))
    println("二次充值人数")
    orderJoin.groupBy("agent_id","server_code").agg(count("agent_id").as("as")).filter($"as">1).count()
    //活跃人数
    println("活跃人数")
    chargeWithAgent.select("agent_id","create_user_type","server_code","province","city","district").union(orderWithAgent.select("agent_id","create_user_type","server_code","province","city","district")).groupBy("create_user_type","server_code","province","city","district").agg(countDistinct("agent_id").as("as")).agg(sum("as")).show()
    //转卡代理数
    println("转卡代理人数")
    chargeWithAgent.filter($"server_code"==="sichuan_db").agg(countDistinct("agent_id")).show()
    val chs = chargeWithAgent.filter($"server_code"==="sichuan_db").groupBy("agent_id").agg(count("agent_id").as("as"))
    val detail = spark.read.table("datasystem.fact_agent_detail_all").filter($"date_dim_id"===732 && $"server_code"==="sichuan_db").filter($"charge_count" > 0)
    detail.join(chs,Seq("agent_id"),"full_outer").filter($"as".isNull || $"charge_count".isNull).show()
    orderWithAgent.filter($"server_code"==="sichuan_db").select("agent_id").union(chargeWithAgent.filter($"server_code"==="sichuan_db").select("agent_id")).agg(countDistinct("agent_id"))
    agentWithArea.filter($"agent_id"==="109044623")

    val nots = detailAll.filter($"charge_count".isNotNull)


    val agents = chargeWithAgent.groupBy("agent_id","create_user_type","server_code","province","city","district").agg(count("agent_id").as("as"))

    val t= agents.join(nots,Seq("agent_id"),"left").filter($"charge_count".isNull)







    //-----------------------------------------------------------------

    //每天的数据
    //充值次数
    orderWithAgent.filter($"target_day"===targetDay && $"server_code"==="sichuan_db").count()
    //充值房卡数
    orderWithAgent.filter($"target_day"===targetDay).agg(sum("cards")).show(false)
    //充值金额
    orderWithAgent.filter($"target_day"===targetDay).agg(sum("money")).show(false)
    //充值代理人数
    orderWithAgent.filter($"target_day"===targetDay).agg(countDistinct("agent_id")).show(false)
    //转卡数量
    chargeWithAgent.filter($"target_day"===targetDay && $"server_code"==="sichuan_db").groupBy("charge_type").agg(sum("card_num")).show()//
    //转卡人数
    val ch = chargeWithAgent.filter($"target_day"===targetDay  && $"server_code"==="sichuan_db").groupBy("agent_id").agg(count("agent_id").as("as"))
    val active = orderWithAgent.filter($"server_code"==="sichuan_db").select("agent_id").union(chargeWithAgent.filter($"server_code"==="sichuan_db").select("agent_id"))
    active.agg(countDistinct("agent_id")).show(false)
    //新增代理
    agentWithArea.filter($"createtime".startsWith("2018-01-01")).agg(countDistinct("agent_id")).show
    //新增有效
    orderWithAgent.filter($"createtime".startsWith("2018-01-01")).agg(countDistinct("agent_id")).show

    3000





    //
    val durationData = spark.read.table("datasystem.fact_agent_detail_count_by_day").filter($"date_dim_id">=getDateDimeIdBefore(732,7) && $"date_dim_id"<=732)
    durationData.filter($"server_code"==="sichuan_db" && $"order_count">0).agg(countDistinct("agent_id")).show(false)
    durationData.filter($"server_code"==="sichuan_db" && $"charge_count">0).agg(countDistinct("agent_id")).show(false)
    durationData.filter($"server_code"==="sichuan_db" && ($"charge_count">0 || $"charge_count">0)).agg(countDistinct("agent_id")).show(false)
    charge7WithAgent.filter($"server_code"==="sichuan_db").agg(countDistinct("agent_id")).show(false)
    spark.close()

  }

  def beforeDays(targetDay:String,days:Int)={
    val format = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance()
    calendar.setTime(format.parse(targetDay))
    calendar.add(Calendar.DAY_OF_MONTH,-days)
    format.format(calendar.getTime)
  }

  def getDateDimeIdBefore(dim:Int,d:Int)={
    dim + 1 - d
  }


  def agentStatistics2(spark:SparkSession,targetDay:String)(agentDF:DataFrame)={
    import spark.implicits._
    val createDay = s"${targetDay.substring(0,4)}-${targetDay.substring(4,6)}-${targetDay.substring(6,8)}"
    val seconds = 12
    agentDF.filter(unix_timestamp($"create_time") < seconds).groupBy("server_code","create_user_type","province","city","district").agg(
      when($"server_code".isNotNull,targetDay).as("target_day"),//当日时间
      count(when($"create_time".startsWith(createDay),"agent_id")).as("new_agents"),//当日新增代理
      count(when($"create_time".startsWith(createDay) && $"order_first_time"===targetDay,"agent_id")).as("new_valid"),//当日新增有效代理
      count(when(($"order_first_time"===targetDay || $"charge_first_time"===targetDay) && ($"order_count" >0 || $"charge_count" >0) ,"agent_id")).as("new_active"),//当日活跃代理人数
      count(when($"order_first_time"===targetDay,"agent_id")).as("first_pay"),//当日首次充值
      count("agent_id").as("all_agents"),//总代理人数
      count(when($"order_count" >0,"agent_id")).as("all_valid"),//有效代理人数
      count(when($"enable" =!="Y","agent_id")).as("all_not_able"),//禁用代理人数
      count(when($"order_count" > 1,"agent_id")).as("all_second_pay"),//二次充值人数
      count(when( $"charge_count" >0 ,"agent_id")).as("all_charge"),//转卡人数
      count(when($"order_count" >0 || $"charge_count" >0 ,"agent_id")).as("all_active")//活跃代理人数
    )
  }
}
