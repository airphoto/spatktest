package com.lhs.scala.bounds

//这是函数function1的写法
// Function的泛型里定义了函数入参和出参分别是“逆变”和“协变”的：
trait function1[-T1,+R]{}

// 所以A=>B这样的函数类型，也可以有继承关系的。

object UppersAndDowns {

  //先看出参类型(协变容易理解些)，
  // A=>B和A=>C两个函数类型;
  //如果C extends B则 A=>C 是 A=>B 的子类型
  class A;class B;class C extends B

  //定义A=>C 的函数
  val t2 = (p:A)=>new C
  //可以把 A=>C 这种类型的函数赋值给 A=>B 类型
  val t3:A=>B = t2
  //或者直接把 t2 转化成 A=>B
  t2.asInstanceOf[A=>B]


  //再看看入参类型，这个是逆变，继承关系正好相反。
  //假设: X=>R,Y=>R 如果 Y extends X则 X=>R 是 Y=>R 的子类型
  class R;class X;class Y extends X

  //定义X=>R类型的函数
  val f1 = (x:X)=>new R
  //把X=>R类型的函数赋值给Y=>R类型的
  val f2:Y=>R = f1

  val x:String => _ = null

}
