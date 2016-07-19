package edu.colostate.cs.dsg.benchmark.storm.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
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
