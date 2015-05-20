package edu.colostate.cs.dsg.benchmark.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.colostate.cs.dsg.benchmark.storm.util.*;
import edu.colostate.cs.dsg.benchmark.storm.util.window.SlidingWindow;
import edu.colostate.cs.dsg.benchmark.storm.util.window.SlidingWindowCallback;
import edu.colostate.cs.dsg.benchmark.storm.util.window.SlidingWindowEntry;

import java.util.List;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 5/20/15
 */
public class MonitoringBolt extends BaseBasicBolt {

    public class StateChangeEntry implements SlidingWindowEntry {

        private long timeStamp;
        private long dt;

        public StateChangeEntry(long timeStamp, long dt) {
            this.timeStamp = timeStamp;
            this.dt = dt;
        }

        public long getTime() {
            return timeStamp;
        }
    }

    private final String inputStreamId;
    private SlidingWindow slidingWindow = null;

    public MonitoringBolt(String inputStreamId) {
        this.inputStreamId = inputStreamId;
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        StateChangeEntry newEntry = new StateChangeEntry(tuple.getLongByField(Constants.DataFields.CORR_TS),
                tuple.getLongByField(Constants.DataFields.CORR_DT));
        slidingWindow.add(newEntry, new SlidingWindowCallback() {
            public void remove(List<SlidingWindowEntry> entries) {
                // do nothing
            }
        });
        StateChangeEntry oldest = (StateChangeEntry) slidingWindow.getOldestEntry();
        if(getChangePercentage(oldest.dt, newEntry.dt) > 0.01){
            // time to emit
            basicOutputCollector.emit(Util.getMonitoringStreamId(inputStreamId), new Values(newEntry.timeStamp));
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Util.getMonitoringStreamId(inputStreamId),
                new Fields(Constants.DataFields.TIMESTAMP));
    }

    private double getChangePercentage(long oldVal, long newVal){
        return Math.abs((newVal - oldVal)/(double)oldVal);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        slidingWindow = new SlidingWindow(24*60*60*1000);
    }
}
