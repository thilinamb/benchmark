package edu.colostate.cs.dsg.benchmark.relay;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author Thilina Buddhika
 */
public class RelayBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(SenderSpout.FIELD_LEN, SenderSpout.FIELD_MSG, SenderSpout.FIELD_TS));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        File file = new File("/tmp/relay-bolt-" + System.currentTimeMillis());
        try {
            file.createNewFile();
        } catch (IOException e) {

        }
    }

    @Override
    public void execute(Tuple tuple) {
        outputCollector.emit(new Values(tuple.getIntegerByField(SenderSpout.FIELD_LEN),
                tuple.getBinaryByField(SenderSpout.FIELD_MSG),
                tuple.getLongByField(SenderSpout.FIELD_TS)));
    }
}
