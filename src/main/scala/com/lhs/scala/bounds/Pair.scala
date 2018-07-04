package com.lhs.scala.bounds

class Pair1[T,S](first:T,secod:S)

//上界,我们必须确定first和second有compareTo方法，所以界定了他的上界
//可以传入string类型的数据，但是传入int类型就会报错，这时候可以使用视图界定,这个时候int类型可以隐式转换成RichInt，RichInt实现了Comparable[Int]
class Pair2[T<:Comparable[T]](first:T,second:T){
  def smaller = if(first.compareTo(second) <0 ) first else second
}


//下届，通常来说，替换进来的类型必须是原类型的超类型
class Pair3[T](first:T,second:T){
  def replaceFirst[R>:T](newFirst:R) = new Pair3[R](first,second)
}


//视图界定：  T<%Comparable 关系 意味着 T可以隐士转换成Comparable[T]
class Pair4[T<%Comparable[T]](first:T,second:T){
  def smaller = if(first.compareTo(second) < 0) first else second
}