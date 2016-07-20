package edu.colostate.cs.dsg.benchmark.util.messaging.client;

/**
 * @author Thilina Buddhika
 */
public class SendUtility {
    public static void sendBytes(String endpoint, byte[] bytes) throws MessagingError {
        ChannelWriterRegistry.getInstance().getConnection(endpoint).writeData(bytes, true);
    }
}
