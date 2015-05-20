package edu.colostate.cs.dsg.benchmark.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Set;
import java.util.TreeSet;

/**
 * Author: Thilina
 * Date: 5/20/15
 */
public class CombinedStateChangeDetectionBolt extends BaseBasicBolt {

    private final String sensorStream1;
    private final String outputStream;

    public CombinedStateChangeDetectionBolt(int sensorStream1Id, int sensorStream2Id) {
        this.sensorStream1 = Util.getEdgeStreamId(sensorStream1Id);
        this.outputStream = Util.getCorrelatedChangeOfStateStreamId(sensorStream1Id, sensorStream2Id);
    }

    private class ComparableStateDetection implements Comparable<ComparableStateDetection> {
        private long timeStamp;
        private long stateChange;

        public ComparableStateDetection(long timeStamp, long stateChange) {
            this.timeStamp = timeStamp;
            this.stateChange = stateChange;
        }


        public int compareTo(ComparableStateDetection o) {
            return new Long(timeStamp).compareTo(o.timeStamp) * (-1);
        }
    }

    private Set<ComparableStateDetection> cache = new TreeSet<ComparableStateDetection>() {
        @Override
        public boolean add(ComparableStateDetection value) {
            if (size() == 100) {
                pollLast();
            }
            return super.add(value);
        }
    };

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (tuple.getSourceStreamId().equals(sensorStream1)) {
            cache.add(new ComparableStateDetection(tuple.getLongByField(Constants.DataFields.TIMESTAMP),
                    tuple.getIntegerByField(Constants.DataFields.EDGE)));
        } else {
            long timeStamp = tuple.getLongByField(Constants.DataFields.TIMESTAMP);
            for (ComparableStateDetection stream1Item : cache) {
                if (stream1Item.timeStamp <= timeStamp) {
                    if (stream1Item.stateChange == tuple.getIntegerByField(Constants.DataFields.EDGE)) {
                        basicOutputCollector.emit(outputStream, new Values(timeStamp, timeStamp - stream1Item.timeStamp));
                    }
                    break;
                }
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(outputStream, new Fields(Constants.DataFields.CORR_TS,
                Constants.DataFields.CORR_DT));
    }
}
