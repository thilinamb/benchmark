package edu.colostate.cs.dsg.benchmark.util.loadprofiles.messaging.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class ChannelConnector {
    private final Logger logger = Logger.getLogger(ChannelConnector.class);
    private Bootstrap bootstrap;

    public ChannelConnector() {
        EventLoopGroup group = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new DataLengthEncoder());
     }

    public ChannelWriter addNewConnection(String serverHost, int serverPort) throws MessagingError {
        try {
            // Make a new connection.
            ChannelFuture f = bootstrap.connect(serverHost, serverPort).sync();
            // wait till connection is complete
            Channel channel = f.channel();
            return new ChannelWriter(channel);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new MessagingError(e.getMessage(), e);
        }
    }
}
