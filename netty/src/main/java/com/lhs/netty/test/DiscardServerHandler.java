package com.lhs.netty.test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

/**
 * 处理服务器端的通道
 *
 * DiscardServerHandler扩展了ChannelInboundHandlerAdapter，它是ChannelInboundHandler的一个实现。
 * ChannelInboundHandler提供了可以覆盖的各种事件处理程序方法。
 * 现在，它只是扩展了ChannelInboundHandlerAdapter，而不是自己实现处理程序接口。
 *
 * 要实现DISCARD协议，只需要忽略所有接收到的数据。让我们从处理程序实现直接开始，这个处理程序实现处理Netty生成的I/O事件。
 */
public class DiscardServerHandler extends ChannelInboundHandlerAdapter{

    /**
     *  每当从客户端接收到新数据时，使用该方法来接收客户端的消息
     *  磁力中接收的消息的类型为 ByteBuf
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //以静默的方式求其接收的数据
        ByteBuf in = (ByteBuf) msg;
        try{
            System.out.println(in.toString(CharsetUtil.US_ASCII));
            System.out.flush();
        }finally {
            ReferenceCountUtil.release(msg);
        }
        System.out.println("Yes,A new Client in = "+ctx.name());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        // 出现异常时关闭连接
        cause.printStackTrace();
        ctx.close();
    }
}
