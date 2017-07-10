package com.lhs.spark.core.customs.add_functions

import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2016/6/22.
 */
class SalesRecord(val id:String,
                   val customerID:String,
                   val itemID:String,
                   val itemValue:Double) extends Comparable[SalesRecord] with Serializable{
  override def compareTo(o: SalesRecord): Int = itemID.compareTo(o.itemID)
}

class IteblogCustomFunctions(rdd: RDD[SalesRecord]){
  def totalSales = rdd.map(_.itemValue).sum()
}

object IteblogCustomFunctions{
  implicit def addIteblogCustomFunctions(rdd:RDD[SalesRecord]) = new IteblogCustomFunctions(rdd)
}
