package com.lhs.scala

import java.io.{PrintWriter, File}
import java.sql.{DriverManager, PreparedStatement, Connection}
import java.util
import java.util.Properties

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
    println("\u4FEE\u6539\u6210\u529F")
//    val filesHere = (new File(".")).listFiles()
//    filesHere.foreach(x=>println(x.getName))
//    val scalaFiles = for{
//      file <- filesHere
//      if file.getName.endsWith(".scala")
//    } yield file
//
//    scalaFiles.foreach(x=>println(x.getName))

    val u = List(1,2,3,6,5)
    u.flatMap(x=>{
      val index = u.indexOf(x)+1
      u.combinations(index).flatMap(x=>{
        if(u.length == index -1)
          Array(x.sorted.mkString(","),x.sorted.mkString("[",",","]"))
        else
          Array(x.sorted.mkString(","))
      })
    }).foreach(println)

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
