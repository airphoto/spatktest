package com.lhs.spark.rpc

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}

/**
  * 第一步： 定义一个HelloEndpoint继承自RpcEndpoint表明可以并发的调用该服务，
  * 如果继承自ThreadSafeRpcEndpoint则表明该Endpoint不允许并发。
  *
  * 还有一个默认的RpcEndpoint叫做RpcEndpointVerifier，
  * 每一个RpcEnv初始化的时候都会注册上这个Endpoint，
  * 因为客户端的调用每次都需要先询问服务端是否存在某一个Endpoint。
  *
  *
  * 和Java传统的RPC解决方案对比，可以看出这里不用定义接口或者方法标示（比如通常的id或者name），
  * 使用scala的模式匹配进行方法的路由。
  * 虽然点对点通信的契约交换受制于语言，
  * 这里就是SayHi和SayBye两个case class，
  * 但是Spark RPC定位于内部组件通信，所以无伤大雅。
  *
  *
  *
  * Endpoint是用来提供服务的，所以要首先运行，并且等待客户端来调用
  *
  *
  * @param rpcEnv 类库中最核心的就是RpcEnv，刚刚提到了这就是ActorSystem，服务端和客户端都可以使用它来做通信。
  *               对于server side来说，RpcEnv是RpcEndpoint的运行环境，
  *               负责RpcEndpoint的整个生命周期管理，
  *               它可以注册或者销毁Endpoint，
  *               解析TCP层的数据包并反序列化，
  *               封装成RpcMessage，
  *               并且路由请求到指定的Endpoint，调用业务逻辑代码，
  *               如果Endpoint需要响应，把返回的对象序列化后通过TCP层再传输到远程对端，
  *               如果Endpoint发生异常，那么调用RpcCallContext.sendFailure来把异常发送回去。
  *
  *               对client side来说，通过RpcEnv可以获取RpcEndpoint引用，也就是RpcEndpointRef的。
  *
  *
  *               RpcEnv是和具体的底层通信模块交互的负责人，它的伴生对象包含创建RpcEnv的方法，
  */
class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint{
  override def onStart(): Unit = {
    println("start the hello endpoint")
  }


  /** 这是应答方式的，可以比作TCP
    * 其中RpcCallContext是用于分离核心业务逻辑和底层传输的桥接方法
    * 这也可以看出Spark RPC多用组合，聚合以及回调callback的设计模式来做OO抽象，
    * 这样可以剥离 业务逻辑->RPC封装（Spark-core模块内）->底层通信（spark-network-common）三者
    * @param context RpcCallContext可以用于回复正常的响应以及错误异常
    *                RpcCallContext也分为了两个子类，
    *                分别是LocalNettyRpcCallContext和RemoteNettyRpcCallContext，
    *                这个主要是框架内部使用，
    *                如果是本地就走LocalNettyRpcCallContext直接调用Endpoint即可，
    *                否则就走RemoteNettyRpcCallContext需要通过RPC和远程交互，
    * @return
    */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) =>{
      println(s"receive $msg")
      context.reply(s"hi $msg") // 回复一个message，也可以使一个case class
      context.sendFailure(new Exception("这是异常")) // 回复一个异常，可以是Exception的子类，
                                              // 由于Spark RPC默认采用Java序列化方式，
                                              // 所以异常可以完整的在客户端还原并且作为cause re-throw出去。
    }
    case SayBye(msg) => {
      println(s"receive $msg")
      context.reply(s"bye $msg")
    }
  }

  // 这是单项方式的，可以比作UDP
//  override def receive: PartialFunction[Any, Unit] = {
//
//  }
  override def onStop(): Unit = {
    println("stop the endpoint")
  }
}

case class SayHi(msg:String)
case class SayBye(msg:String)

/**
  * 第二步：把刚刚开发好的Endpoint交给Spark RPC管理其生命周期，用于响应外部请求。
  * RpcEnvServerConfig可以定义一些参数、server名称（仅仅是一个标识）、bind地址和端口。
  * 通过NettyRpcEnvFactory这个工厂方法，生成RpcEnv，RpcEnv是整个Spark RPC的核心所在
  * 通过setupEndpoint将”hello-service”这个名字和第一步定义的Endpoint绑定，
  * 后续client调用路由到这个Endpoint就需要”hello-service”这个名字。
  * 调用awaitTermination来阻塞服务端监听请求并且处理。
  */
object HelloServer{
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val config = RpcEnvServerConfig(new RpcConf(),"hello-server",host,52345)
    val rpcEnv = NettyRpcEnvFactory.create(config)
    val helloEndPoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("hello-service",helloEndPoint)
    rpcEnv.awaitTermination()
  }
}