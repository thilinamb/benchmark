package edu.colostate.cs.dsg.benchmark.util.loadprofiles.messaging.client;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class ChannelWriterRegistry {

    private static final ChannelWriterRegistry instance = new ChannelWriterRegistry();
    private ChannelConnector channelConnector = new ChannelConnector();
    private Map<String, ChannelWriter> channelWriterMap = new ConcurrentHashMap<>();
    private Logger logger = Logger.getLogger(ChannelWriterRegistry.class);

    private ChannelWriterRegistry() {
    }

    public static ChannelWriterRegistry getInstance() {
        return instance;
    }

    public ChannelWriter getConnection(String endpoint) throws MessagingError {
        if (channelWriterMap.containsKey(endpoint)) {
            return channelWriterMap.get(endpoint);
        } else {
            synchronized (this) {
                if (!channelWriterMap.containsKey(endpoint)) {
                    String[] endpointSegments = endpoint.split(":");
                    ChannelWriter channelWriter = channelConnector.addNewConnection(endpointSegments[0],
                            Integer.parseInt(endpointSegments[1]));
                    channelWriterMap.put(endpoint, channelWriter);
                    return channelWriter;
                } else {
                    return channelWriterMap.get(endpoint);
                }
            }
        }
    }
}

