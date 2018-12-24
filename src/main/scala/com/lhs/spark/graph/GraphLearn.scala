package com.lhs.spark.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object GraphLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("error")
    import spark.implicits._

    val myVertices = sc.makeRDD(Array(
      (1L,"Ann"),
      (2L,"Bill"),
      (3L,"Charles"),
      (4L,"Diance"),
      (5L,"Went to gym this morning")))

    val  myEdges  =  sc.makeRDD(Array(
      Edge(1L,2L,"is-friends-with"),
      Edge(2L,3L,"is-friends-with"),
      Edge(3L,4L,"is-friends-with"),
      Edge(4L,5L,"Likes-status"),
      Edge(3L,5L,"Wrote - status")))

    val graph = Graph(myVertices,myEdges)

//    graph.triplets.collect().foreach(println)

    // 转换边
//    graph.mapTriplets(t=>(t.attr,t.attr=="is-friends-with" && t.srcAttr.toLowerCase.contains("a"))).triplets

    // 计算每个顶点的出度：让每条表发出消息到管理的源顶点
    val sendMsgFun = (sedMsg:EdgeContext[String,String,Int])=>sedMsg.sendToSrc(1)
    val sumFun = (mesg1:Int,mesg2:Int) => mesg1 + mesg2
    graph.aggregateMessages[Int](sendMsgFun,sumFun).collect().foreach(println)
    graph.aggregateMessages[Int](sendMsgFun,sumFun).join(graph.vertices).collect().foreach(println)
    graph.aggregateMessages[Int](sendMsgFun,sumFun).rightOuterJoin(graph.vertices).map(x=>(x._2._2,x._2._1.getOrElse(0)))

    val sendMsg2Dst = (sendMsg:EdgeContext[Int,String,Int]) => sendMsg.sendToDst(sendMsg.srcAttr + 1)
    val msergeMsg = (a:Int,b:Int) => a max b

  }

//  def propateEdgeCount(g:Graph[Int,String],sendMsg2Dst:(EdgeContext[Int,String,Int])=>Unit,mergeMsg:(Int,Int)=>Int):Graph[Int,String]={
//    val verts = g.aggregateMessages(sendMsg2Dst,mergeMsg)
//    val g2 = Graph(verts,g.edges)
//
////    val check = g2.vertices.join(g.vertices).map(x=>x._2._1-)
//  }


}
