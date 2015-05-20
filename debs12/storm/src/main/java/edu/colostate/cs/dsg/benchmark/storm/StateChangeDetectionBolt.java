package edu.colostate.cs.dsg.benchmark.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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
