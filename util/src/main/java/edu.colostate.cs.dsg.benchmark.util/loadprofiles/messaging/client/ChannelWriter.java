package edu.colostate.cs.dsg.benchmark.util.loadprofiles.messaging.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author Thilina Buddhika
 */
public class ChannelWriter {
    private static final long FLUSHING_THRESHOLD = 1024 * 512; // 1
    private long counter;
    private Channel channel;

    public ChannelWriter(Channel channel) {
        this.channel = channel;
    }

    public void writeData(byte[] payload, boolean immediately) {
        ChannelFuture future = channel.write(payload);
        channel.flush();
        counter += payload.length;
        if (immediately || counter >= FLUSHING_THRESHOLD) {
            try {
                future.sync();
            } catch (InterruptedException ignore) {
                ignore.printStackTrace();
            }
            counter = 0;
        }
    }
}

