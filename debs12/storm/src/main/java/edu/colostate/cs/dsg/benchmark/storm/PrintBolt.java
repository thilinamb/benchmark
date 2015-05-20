package edu.colostate.cs.dsg.benchmark.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class PrintBolt extends BaseBasicBolt {

    private static final Logger LOGGER = Logger.getLogger(PrintBolt.class);

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println(tuple.getLongByField(Constants.DataFields.TIMESTAMP) + ": "  +
                tuple.getIntegerByField(Constants.DataFields.EDGE));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
