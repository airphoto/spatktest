package com.lhs.scala

import java.io.{File, PrintWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util
import java.util.Properties

import org.apache.hadoop.mapreduce.v2.proto.MRProtos.StringCounterGroupMapProto
import redis.clients.jedis.Jedis

/**
 * Created by Administrator on 2017/6/26.
 */

/**
 * <P>@ObjectName : Test</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/6/26 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object Test {
  def main(args: Array[String]) {
//    println("\u4FEE\u6539\u6210\u529F")
//    val filesHere = (new File(".")).listFiles()
//    filesHere.foreach(x=>println(x.getName))
//    val scalaFiles = for{
//      file <- filesHere
//      if file.getName.endsWith(".scala")
//    } yield file
//
//    scalaFiles.foreach(x=>println(x.getName))

 /*   val u = List(1,2,3,6,5)
    u.flatMap(x=>{
      val index = u.indexOf(x)+1could not return the resource to the pool
      u.combinations(index).flatMap(x=>{
        if(u.length == index -1)
          Array(x.sorted.mkString(","),x.sorted.mkString("[",",","]"))
        else
          Array(x.sorted.mkString(","))
      })
    }).foreach(println)
*/

    val jedis = new Jedis("10.27.90.182",9597)
    jedis.auth("yunweiCEshi")
    jedis.select(7)
    val keys = jedis.keys("*")
    println(keys.size())
    var bsSum = 0
    var jsSum = 0
    keys.toArray().sortWith((x,y)=> x.toString.compareTo(y.toString)<0).foreach(r=>{
    val key = r.toString
      if(key.contains("person")) {
        println(key,jedis.scard(key))
      }else if(key.contains("bswf:bs") && key.contains(":0:1")){
        bsSum = bsSum + jedis.hgetAll(key).getOrDefault("15001","0").toInt
        println(key,jedis.hgetAll(key).getOrDefault("15001","0").toInt)
      }else if(key.contains("bswf:js") && key.contains(":0:")){
        jsSum = jsSum + jedis.hgetAll(key).getOrDefault("15001","0").toInt
        println(key,jedis.hgetAll(key).getOrDefault("15001","0"))
      }else{
//        println(key,jedis.get(key))
      }
//      jedis.del(key)
    })
    jedis.close()
    println("bs:"+bsSum)
    println("js:"+jsSum)
//println(true || true && false)
  }

  //找最大公约数
  def gcd(a:Int,b:Int):Int={
    if(b==0) a else gcd(b,a%b)
  }

  def withPrintWriter(file: File,op:PrintWriter=>Unit): Unit ={
    val writer = new PrintWriter(file)
    try{
      op(writer)
    }finally {
      writer.close()
    }
  }


  def withInsertData(connection: Connection,preparedStatement: PreparedStatement)(op:(Connection,PreparedStatement)=>Unit): Unit ={

    try{
      op(connection,preparedStatement)
    }finally {
      connection.close()
    }
  }


}
