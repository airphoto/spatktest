package com.lhs.utils


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

  /**
    * 根据文件大小制定分区的个数
    */
  private def getPartitionsByDirLen(sparkSession: SparkSession,beforeTarget:String,path: String):Map[String,Int] ={

    val dataPath = new Path(path)
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val blockSize = sparkSession.sparkContext.getConf.get("pipeline.block.size","128").toLong
    fs.listStatus(dataPath)
      .map(x=> {
        val logType = x.getPath.getName
        val logPath = new Path(s"${x.getPath.toString}/$beforeTarget")

        val contentSize = try{
          fs.getContentSummary(logPath).getLength
        }catch {
          case e:Exception=>{
            println(s"数据类型 ：$logType 没有数据：${logPath.toString} ")
            1L
          }
        }

        val sizeM = contentSize / 1024 / 1024 / blockSize

        val partitions = if(sizeM < 1) 1 else sizeM.toInt
        (x.getPath.getName, partitions)
      }).toMap
  }
}
