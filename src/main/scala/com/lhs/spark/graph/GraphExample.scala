package com.lhs.spark.graph

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").appName("GraphExample").getOrCreate()
    val sc = spark.sparkContext

//    val graph = createGraph(sc)
//    graphAttributes(graph)
//    graphTruplet(graph)
    pregelTest(sc)
    spark.close()
  }

  //使用了 Edge case 类。边缘具有 srcId 和 dstId 对应于源和目标顶点标识符。此外， Edge 该类有一个 attr 存储边缘属性的成员。
  def createGraph(sc:SparkContext)={
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    Graph(users, relationships, defaultUser)
  }

  def graphAttributes(graph: Graph[(String,String),String])={
    //定点操作
    val verts = graph.vertices
    verts.filter{case (id,(name,pos))=>pos=="prof"}.foreach(println)
    //边的操作
    val edge = graph.edges
    edge.filter(e=>e.srcId>e.dstId).foreach(println)
  }

  def graphTruplet(graph: Graph[(String,String),String])={
    val triplet = graph.triplets
    val tripMap = triplet.map(trip=>"src attr : "+trip.srcAttr +" ; attr : "+trip.attr +"; des attr : "+trip.dstAttr)
    tripMap.collect().foreach(println)
  }

  def graphOpertions(graph: Graph[(String,String),String])={
    //图属性
    println("边的个数："+graph.numEdges)
    println("定点的个数："+graph.numVertices)
    println("入度："+graph.inDegrees)
    println("出度："+graph.outDegrees)
    println("度："+graph.degrees)

    //视图
    val vertices = graph.vertices
    vertices.collect().foreach(v=>println("顶点 ："+v))

    val edge = graph.edges
    edge.collect().foreach(e=>println("边 ："+e))

    val trip = graph.triplets
    trip.collect().foreach(t=>println("两点："+trip))

    //缓存图
    graph.cache()
    graph.unpersist()

    //转换顶点和边的属性，这种转换可以保留原始图的结构索引
    graph.mapVertices{case (id,(name,pos))=>}
    graph.mapEdges(e=>e)
    graph.mapTriplets(t=>t)

    //修改图的结构
    val reverse = graph.reverse

    //join rdd

    // 关于 triplet 的汇总信息
//    graph.collectNeighborIds()
  }

  //Pregel 运算符来表达单源最短路径的计算。
  def pregelTest(sc:SparkContext)={
    val graph = GraphGenerators.logNormalGraph(sc,10)
    val edges = graph.mapEdges(e => e.attr.toDouble)
    val sourceID:VertexId = 4
    val initialGraph = graph.mapVertices((id,_)=>{
      if(id==sourceID) 0.0 else Double.PositiveInfinity
    })

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id,dist,newDist) => math.min(dist,newDist),
      triplet=>{
        if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
          Iterator((triplet.dstId,triplet.srcAttr+triplet.attr))
        }else{
          Iterator.empty
        }
      },
      (a,b)=>math.min(a,b)
    )

    println(sssp.vertices.collect.sortBy(_._1).mkString("\n"))
  }

  def testEdgeListFile(sc:SparkContext){
    val graph = GraphLoader.edgeListFile(sc,"")
  }
}
