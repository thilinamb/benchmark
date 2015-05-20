package edu.colostate.cs.dsg.benchmark.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class ReportBolt extends BaseBasicBolt {

    private static final Logger LOGGER = Logger.getLogger(ReportBolt.class);

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOGGER.info(tuple.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
