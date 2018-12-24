package com.lhs.netty.test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author lihuasong
 * @description 这个是用来实现ECHO协议，这个协议的作用就是将客户端输入的信息全部返回
 * @create 2018/11/16
 **/
public class EchoServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // ChannelHandlerContext 提供各种不同的操作用于触发不用的IO的时间和操作
        // write方法用逐字返回接收的消息
        // 这里我们不需要在DSICARD例子中那样调用释放，因为netty会在写的时候自动释放
        // 只调用write是不会释放的，它只会缓存，flush才会释放
        ctx.write(msg);
        ctx.flush();
        // 也可以直接这样操作ctx.writeAndFlush(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
