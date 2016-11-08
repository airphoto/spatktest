package com.lhs.spark.hbase

import java.io.Closeable

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.io.IOUtils

/**
 * Created by Administrator on 2016/11/8.
 */
object HBaseUtils {
  def getConnection={
    val conf = HBaseConfiguration.create()
    ConnectionFactory.createConnection(conf)
  }

  def getTable(connection:Connection,name:String)={
    connection.getTable(TableName.valueOf(name))
  }

  def close(closeable:Closeable*): Unit ={
    closeable.filter(x=> x != null).foreach(IOUtils.closeStream)
  }
}
