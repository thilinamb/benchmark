package edu.colostate.cs.dsg.benchmark.relay;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * @author Thilina Buddhika
 */
public class SenderSpout extends BaseRichSpout {

    public static final String FIELD_LEN = "length";
    public static final String FIELD_MSG = "payload";
    public static final String FIELD_TS = "ts";
    private SpoutOutputCollector collector;
    private long counter = 0l;
    private Random rand = new Random();
    private int length;
    private int lengthIndex = -1;
    private int[] lengths = new int[]{38, 38, 88, 188, 388, 1012, 4084, 10228};

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FIELD_LEN, FIELD_MSG, FIELD_TS));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        File file = new File("/tmp/sender-spout-" + System.currentTimeMillis());
        try {
            file.createNewFile();
        } catch (IOException e) {

        }
    }

    public SenderSpout(int length) {
        this.length = length;
    }

    public SenderSpout(){

    }

    @Override
    public void nextTuple() {
        /*if(counter % 10000000 == 0){
            lengthIndex = Math.min((lengthIndex+1), lengths.length - 1);
            length = lengths[lengthIndex];
        }*/

        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        collector.emit(new Values(length, bytes,System.currentTimeMillis()));
        counter++;
        /*try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }
}
