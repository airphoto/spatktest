package com.lhs.spark.core.customs.add_functions


import com.lhs.com.lhs.java.TestObjestsss
object Test {
  def main(args: Array[String]) {


    val ids = Array("key").map(_.asInstanceOf[Object])

    val list = TestObjestsss.findByClickids(ids)
    print(list.get(0).get("key"))
  }
}
