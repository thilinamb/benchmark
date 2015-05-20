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
        builder.setSpout("file-input-spout", new FileReaderSpout(), 1);
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
        builder.setBolt("print-bolt", new PrintBolt(), 1).globalGrouping("BM05-Bolt", Util.getEdgeStreamId(5));
        builder.setBolt("print-bolt2", new PrintBolt(), 1).globalGrouping("BM06-Bolt", Util.getEdgeStreamId(6));
        builder.setBolt("print-bolt3", new PrintBolt(), 1).globalGrouping("BM07-Bolt", Util.getEdgeStreamId(7));
        builder.setBolt("print-bolt4", new PrintBolt(), 1).globalGrouping("BM08-Bolt", Util.getEdgeStreamId(8));
        builder.setBolt("print-bolt5", new PrintBolt(), 1).globalGrouping("BM09-Bolt", Util.getEdgeStreamId(9));
        builder.setBolt("print-bolt6", new PrintBolt(), 1).globalGrouping("BM10-Bolt", Util.getEdgeStreamId(10));
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
