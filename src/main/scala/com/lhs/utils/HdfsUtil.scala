package com.lhs.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object HdfsUtil {

  def delIfExists(path:String,spark:SparkSession)={
    val hdfsPath = new Path(path)
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fileSystem.exists(hdfsPath)) fileSystem.delete(hdfsPath,true)
    else false
  }
}
