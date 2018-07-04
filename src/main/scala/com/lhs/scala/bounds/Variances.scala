package com.lhs.scala.bounds

//在类型定义时(declaration-site)声明为协变
trait A[+T]
//要注意variance并不会被继承，父类声明为variance，子类如果想要保持，仍需要声明
//协变：声明的时候用父类，传值的时候用子类
class C[+T] extends A[T]

class X; class Y extends X;

//逆变：声明的时候用子类，传值的时候用父类
trait B[-T]
class D[-T] extends B[T]



object Variances {
  def main(args: Array[String]): Unit = {
    val t:C[X] = new C[Y]
    val t2:D[Y] = new D[X]
//    Array("").foldLeft()

    println(t,t2)

  }
}
