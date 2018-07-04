package com.lhs.scala.bounds

import java.io.{ByteArrayInputStream, Closeable, InputStream}
import java.util.zip.GZIPInputStream

import org.apache.commons.codec.binary.Base64InputStream
import org.apache.commons.io.IOUtils


// 方法中的参数类型声明时必须符合逆变（或不变），
// 以让子类方法可以接收更大的范围的参数(处理能力增强)；
// 而不能声明为协变，
// 子类方法可接收的范围是父类中参数类型的子集(处理能力减弱)。
class In[-A] {def fun(x:A){}}


case class Closable[A<:{def close():Unit},B](a:A){
  def map[B](f:A=>B):B={
    try f(a)
    finally {
      if (a!=null) a.close()
    }
  }

  def flatMap[B](f:A=>B) = map(f)
}

object WIthTest {
  def main(args: Array[String]): Unit = {
    val line = analysis("H4sIAAAAAAAC/6tWKsgpTc/M88/LycxLVbKKrlZKLCiA8fJKc3J0oCo8U5SsDA0NDGp1CCkxJKzEiLASY8JKTAgrMSWsxIywEnPCSiwIK7EkoMTIgGDQGRkTjACgEiJMMSKsxLg2tpYLAC+otRIgAgAA") _
    println(line(IOUtils.toString))
  }

  def analysis(line:String)(ts:InputStream=>String)={
    Closable(new ByteArrayInputStream(line.getBytes())).flatMap{
      bais => Closable(new Base64InputStream(bais)).flatMap{
        b64Io=> Closable(new GZIPInputStream(b64Io)).map{
          rs=>ts{rs}
        }
      }
    }
  }
}
