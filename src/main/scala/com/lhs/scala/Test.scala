package com.lhs.scala

import java.io.File

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
//    println(gcd(66,42))
    val filesHere = (new File(".")).listFiles()
    filesHere.foreach(x=>println(x.getName))
    val scalaFiles = for{
      file <- filesHere
      if file.getName.endsWith(".scala")
    } yield file

    scalaFiles.foreach(x=>println(x.getName))
  }

  //找公约数
  def gcd(a:Int,b:Int):Int={
    if(b==0) a else gcd(b,a%b)
  }
}
