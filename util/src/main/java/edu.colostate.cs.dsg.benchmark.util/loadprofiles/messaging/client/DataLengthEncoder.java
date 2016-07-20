package edu.colostate.cs.dsg.benchmark.util.loadprofiles.messaging.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author Thilina Buddhika
 */
@ChannelHandler.Sharable
public class DataLengthEncoder extends MessageToByteEncoder<byte[]> {
    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, byte[] bytes, ByteBuf byteBuf)
            throws Exception {
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }
}
