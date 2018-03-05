package com.lhs.scala.typeabout

object AboutTypes {
  def main(args: Array[String]): Unit = {
    val tmp = Map("t1"->1)
  }

  def getDataByType[T](array:Array[String])={
    array.map(_.toInt)
  }

  def encoderByMD5(str: String)={
    import sun.misc.BASE64Encoder
    import java.security.MessageDigest
    //确定计算方法
    val md5 = MessageDigest.getInstance("MD5")
    val base64en = new BASE64Encoder
    //加密后的字符串
    base64en.encode(md5.digest(str.getBytes("utf-8")))
  }
}
