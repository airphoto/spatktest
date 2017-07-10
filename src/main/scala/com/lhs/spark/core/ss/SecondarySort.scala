package com.lhs.spark.core.ss

/**
 * Created by Administrator on 2016/12/27.
 */
class SecondarySort(val first:Int,val second:Int) extends Ordered[SecondarySort] with Serializable{
  override def compare(that: SecondarySort): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}

object SecondarySort{
  def apply(first:Int,second:Int)={
    new SecondarySort(first,second)
  }
}
