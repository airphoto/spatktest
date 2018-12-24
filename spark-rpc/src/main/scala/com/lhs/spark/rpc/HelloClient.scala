package com.lhs.spark.rpc

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
/**
  * 第三步，开发一个client调用刚刚启动的server，
  * 首先RpcEnvClientConfig和RpcEnv都是必须的，
  * 然后通过刚刚提到的”hello-service”名字新建一个远程Endpoint的引用（Ref），
  * 可以看做是stub，用于调用，
  *
  *
  *
  * RpcEndpointRef类似于Akka中ActorRef，
  * 顾名思义，它是RpcEndpoint的引用，
  * 提供的方法send等同于!,
  * ask方法等同于?，
  *
  * send用于单向发送请求（RpcEndpoint中的receive响应它），提供fire-and-forget语义，
  * 而ask提供请求响应的语义（RpcEndpoint中的receiveAndReply响应它），默认是需要返回response的，带有超时机制，可以同步阻塞等待，也可以返回一个Future句柄，不阻塞发起请求的工作线程。
  *
  * RpcEndpointRef是客户端发起请求的入口，它可以从RpcEnv中获取，并且聪明的做本地调用或者RPC。
  *
  * @author lihuasong
  *
  *         2018-11-19 13:08
  **/
object HelloClient {
  def main(args: Array[String]): Unit = {
    syncCall()
  }

  //这里首先展示通过异步的方式来做请求。
  def asyncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val future = endPointRef.ask[String](SayHi("neo"))
    endPointRef.send("")
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  // 这是同步请求
  def syncCall() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "hello-client")
    val rpcEnv = NettyRpcEnvFactory.create(config)
    val endPointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 52345), "hello-service")
    val result = endPointRef.askWithRetry[String](SayBye("neo"))
    println(result)
  }
}
