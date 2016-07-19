
package edu.colostate.cs.dsg.benchmark.relay;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class CounterBolt extends BaseRichBolt {

    private static Logger LOGGER = Logger.getLogger(CounterBolt.class);


    private long receiverCount = 0;
    private long lastTimeStamp = 0;
    private long MILESTONE = 10000;
    private OutputCollector outputCollector;
    private BufferedWriter writer;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // none. End of topology
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        try {
            writer = new BufferedWriter(new FileWriter("/tmp/storm-stats-" + System.currentTimeMillis()));
        } catch (IOException e) {
            LOGGER.error(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        receiverCount++;
        // rough performance metrics
        if (lastTimeStamp == 0) {
            lastTimeStamp = System.currentTimeMillis();
        } else if (receiverCount % MILESTONE == 0) {
            long now = System.currentTimeMillis();
            long timeStamp = tuple.getLongByField(SenderSpout.FIELD_TS);
            try {
                writer.write(tuple.getIntegerByField(SenderSpout.FIELD_LEN) + "," + (double) MILESTONE * 1000 / (now - lastTimeStamp)
                        + "," + (now - timeStamp) + "," +
                        (((double)((tuple.getIntegerByField(SenderSpout.FIELD_LEN)+12) * MILESTONE * 8 * 1000)) / ((1000 * 1000 * 1000) * (now - lastTimeStamp))) + "\n");
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println("Number of received messages: " + receiverCount + ", Time since last log:" + (now - lastTimeStamp) +
            //        ", Latency: " + (now - timeStamp) + ", Throughput: " + (double)MILESTONE*1000/(now - lastTimeStamp));
            lastTimeStamp = now;
        }
    }
}
