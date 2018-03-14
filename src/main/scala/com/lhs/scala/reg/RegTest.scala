package com.lhs.scala.reg

object RegTest {
  def main(args: Array[String]): Unit = {
    val pattern = """\"time\":\s*\"(\d\d\d\d)-(\d\d)-(\d\d)""".r
    val str = "{\"type\": \"login\",\"version\": \"1.00.00\",\"time\": \"2018-01-01 11:09:00 000\",\"ip\": \"210.12.30.134\",\"properties\": {\"user\": {\"gid\": 12,\"appId\": 10001,\"userId\": 902522,\"pluginId\":1},\"serverId\": 88888,\"isGuildMember\": true,\"isFirstTimeFromThisPlugin\": true,\"clientIP\": \"61.135.169.125\"}}"
    println(pattern findAllIn(str) toList)
  }
}
