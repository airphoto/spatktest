package com.lhs.spark.core.customs.rdd

import com.lhs.spark.core.customs.add_functions.SalesRecord
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2017/6/22.
 */
class IteblogDiscountRDD(pre:RDD[SalesRecord],discountPercentage:Double) extends RDD[SalesRecord](pre){
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
    firstParent[SalesRecord].iterator(split,context).map(salesRdcord=>{
      val discount = salesRdcord.itemValue * discountPercentage
      new SalesRecord(salesRdcord.id,salesRdcord.customerID,salesRdcord.itemID,discount)
    })
  }

  override protected def getPartitions: Array[Partition] = firstParent[SalesRecord].partitions
}