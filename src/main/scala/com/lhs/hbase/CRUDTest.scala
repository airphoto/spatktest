package com.lhs.hbase

import java.io.Closeable

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.IOUtils

/**
  * Created by abel on 16-11-6.
  */
object CRUDTest {

  def getConnection={
    val conf = HBaseConfiguration.create()
    ConnectionFactory.createConnection(conf)
  }

  def getAdmin={
    getConnection.getAdmin
  }

  def getTable(name:String)={
    getConnection.getTable(TableName.valueOf(name))
  }

  def closeResource(closeables:Closeable*): Unit ={
    closeables.filter(x=>x != null).foreach(x=>IOUtils.closeStream(x))
  }


  def main(args: Array[String]) {
    val table = getTable("callingnumber_count_day")
    val put = new Put(Bytes.toBytes("1003"))
    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes("wangwu"))
    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes("29"))
    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("sex"),Bytes.toBytes("man"))
    table.put(put)
    closeResource(table)
  }

}
