package com.lhs.spark.ml.svd

import org.apache.spark.sql.SparkSession
import java.awt.image.BufferedImage
import java.io.{InputStream, InputStreamReader}
import java.net.URL

import org.apache.hadoop.fs.{FileSystem, Path}
object SvdPca {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("svd").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val path = "/test/input/lfw/*"
    val rdd = sc.wholeTextFiles(path)
    val first = rdd.first()
    println(first)

    val files = rdd.map { case (fileName, content) => fileName}
//    val files = rdd.map { case (fileName, content) => fileName.replace("hdfs://emr-header-1.cluster-52120:9000", "") }
    println(files.first)

    println(files.count)

    val aePath = "hdfs://10.51.237.173:9000/test/input/lfw/Aaron_Eckhart/Aaron_Eckhart_0001.jpg"
    val hdfsPath = new Path(aePath)
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val aeImage = loadImageFromFile(aePath,fileSystem)
    val grayImage = processImage(aeImage, 100, 100)
    import javax.imageio.ImageIO
    ImageIO.write(grayImage, "jpg", fileSystem.create(new Path("/test/output/image/aegray.jpg")))
    spark.close()
  }

  def loadImageFromFile(path: String,fileSystem: FileSystem): BufferedImage = {
    import javax.imageio.ImageIO
    import java.io.File
    ImageIO.read(fileSystem.open(new Path(path)))
  }

  def processImage(image: BufferedImage, width: Int, height: Int): BufferedImage = {
    val bwImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g = bwImage.getGraphics()
    g.drawImage(image, 0, 0, width, height, null)
    g.dispose()
    bwImage
  }
}
