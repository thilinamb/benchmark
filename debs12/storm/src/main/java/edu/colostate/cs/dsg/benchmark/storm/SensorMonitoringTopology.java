package edu.colostate.cs.dsg.benchmark.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class SensorMonitoringTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        // input
        builder.setSpout("file-input-spout", new FileReaderSpout(), 1);
        // phase 1
        builder.setBolt("BM05-Bolt", new StateChangeDetectionBolt(5), 1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_05);
        builder.setBolt("BM06-Bolt", new StateChangeDetectionBolt(6), 1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_06);
        builder.setBolt("BM07-Bolt", new StateChangeDetectionBolt(7), 1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_07);
        builder.setBolt("BM08-Bolt", new StateChangeDetectionBolt(8), 1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_08);
        builder.setBolt("BM09-Bolt", new StateChangeDetectionBolt(9), 1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_09);
        builder.setBolt("BM10-Bolt", new StateChangeDetectionBolt(10), 1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_10);
        // phase 2
        builder.setBolt("BM58-Bolt", new CombinedStateChangeDetectionBolt(5,8), 1).globalGrouping("BM05-Bolt", Util.getEdgeStreamId(5)).
                globalGrouping("BM08-Bolt", Util.getEdgeStreamId(8));
        builder.setBolt("BM69-Bolt", new CombinedStateChangeDetectionBolt(6,9), 1).globalGrouping("BM06-Bolt", Util.getEdgeStreamId(6)).
                globalGrouping("BM09-Bolt", Util.getEdgeStreamId(9));
        builder.setBolt("BM710-Bolt", new CombinedStateChangeDetectionBolt(7,10), 1).globalGrouping("BM07-Bolt", Util.getEdgeStreamId(7)).
                globalGrouping("BM10-Bolt", Util.getEdgeStreamId(10));
        // phase 3 - Monitoring
        builder.setBolt("Monitoring-58", new MonitoringBolt(Util.getCorrelatedChangeOfStateStreamId(5,8)), 1).
                globalGrouping("BM58-Bolt", Util.getCorrelatedChangeOfStateStreamId(5,8));
        builder.setBolt("Monitoring-69", new MonitoringBolt(Util.getCorrelatedChangeOfStateStreamId(6,9)), 1).
                globalGrouping("BM69-Bolt", Util.getCorrelatedChangeOfStateStreamId(6,9));
        builder.setBolt("Monitoring-710", new MonitoringBolt(Util.getCorrelatedChangeOfStateStreamId(7,10)), 1).
                globalGrouping("BM710-Bolt", Util.getCorrelatedChangeOfStateStreamId(7,10));

        builder.setBolt("Report-Bolt", new ReportBolt(), 1).
                globalGrouping("Monitoring-58", Util.getMonitoringStreamId(Util.getCorrelatedChangeOfStateStreamId(5,8))).
                globalGrouping("Monitoring-69", Util.getMonitoringStreamId(Util.getCorrelatedChangeOfStateStreamId(6,9))).
                globalGrouping("Monitoring-710", Util.getMonitoringStreamId(Util.getCorrelatedChangeOfStateStreamId(7,10)));

        Config conf = new Config();
        //conf.put(Config.TOPOLOGY_DEBUG, false);

        conf.setMaxTaskParallelism(20);
        LocalCluster cluster = new LocalCluster();
        conf.put(Constants.INPUT_FILE_PATH,args[1]);
        cluster.submitTopology(args[0], conf, builder.createTopology());
        try {
            Thread.sleep(1200 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();
    }
}
