package edu.colostate.cs.dsg.benchmark.storm.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import edu.colostate.cs.dsg.benchmark.storm.util.Util;
import edu.colostate.cs.dsg.benchmark.storm.util.Constants;

/**
 * Detects the initial state change.
 * Author: Thilina
 * Date: 5/19/15
 */
public class StateChangeDetectionBolt extends BaseBasicBolt {

    private int previousState = -1;
    private final String edgeStreamId;

    public StateChangeDetectionBolt(int operatorId) {
        this.edgeStreamId = Util.getEdgeStreamId(operatorId);
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int currentState = tuple.getIntegerByField(Constants.DataFields.BM_VAL);
        if(previousState == -1){
            previousState = currentState;
        } else if (previousState != currentState) {
            if(currentState == 1){
                basicOutputCollector.emit(edgeStreamId,
                        new Values(tuple.getLongByField(Constants.DataFields.TIMESTAMP), 1));
            } else {
                basicOutputCollector.emit(edgeStreamId,
                        new Values(tuple.getLongByField(Constants.DataFields.TIMESTAMP), 0));
            }
            previousState = currentState;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(edgeStreamId,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.EDGE));
    }
}
