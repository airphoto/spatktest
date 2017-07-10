package com.lhs.scala.extend

/**
 * Created by Administrator on 2017/1/7.
 */
abstract class Element {
  def contents:Array[String] //只声明，没有定义

  def height:Int=contents.length

  def width:Int=if(height == 0) 0 else contents(0).length

  def above(that:Element):Element=Element.elem(this.contents ++ that.contents)

  def beside(that:Element):Element=Element.elem(
  for((line1,line2)<-this.contents zip that.contents)yield line1+line2
  )

  override def toString=contents.mkString("\n")
}

object Element{

  private class ArrayElement(val contents:Array[String]) extends Element

  private class LineElement(s:String) extends ArrayElement(Array(s)){
    override val width = s.length
    override val height = 1
  }

  private class UniformElement(
    char: Char,
    override val width:Int,
    override val height:Int
    )extends Element{
    private val line = char.toString * width
    override def contents: Array[String] = Array.fill(height)(line)
  }

  def elem(contents:Array[String]):Element = new ArrayElement(contents)

  def elem(chr:Char,width:Int,height:Int):Element=new UniformElement(chr,width,height)

  def elem(line:String):Element = new LineElement(line)

  def main(args: Array[String]) {
    val line1 = Element.elem("ths")
    val line2 = Element.elem("that")
    println(line1 above line2)
  }
}
