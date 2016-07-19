package edu.colostate.cs.dsg.benchmark.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import edu.colostate.cs.dsg.benchmark.storm.bolt.CombinedStateChangeDetectionBolt;
import edu.colostate.cs.dsg.benchmark.storm.bolt.MonitoringBolt;
import edu.colostate.cs.dsg.benchmark.storm.bolt.ReportBolt;
import edu.colostate.cs.dsg.benchmark.storm.bolt.StateChangeDetectionBolt;
import edu.colostate.cs.dsg.benchmark.storm.spout.FileReaderSpout;
import edu.colostate.cs.dsg.benchmark.storm.util.Constants;
import edu.colostate.cs.dsg.benchmark.storm.util.Util;
import org.apache.log4j.Logger;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class SensorMonitoringTopology {

    private static final Logger LOGGER = Logger.getLogger(SensorMonitoringTopology.class);

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        // input
        builder.setSpout("file-input-spout", new FileReaderSpout(), 1);
        // phase 1
        builder.setBolt("BM05-Bolt", new StateChangeDetectionBolt(5), 1).setNumTasks(1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_05);
        builder.setBolt("BM06-Bolt", new StateChangeDetectionBolt(6), 1).setNumTasks(1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_06);
        builder.setBolt("BM07-Bolt", new StateChangeDetectionBolt(7), 1).setNumTasks(1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_07);
        builder.setBolt("BM08-Bolt", new StateChangeDetectionBolt(8), 1).setNumTasks(1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_08);
        builder.setBolt("BM09-Bolt", new StateChangeDetectionBolt(9), 1).setNumTasks(1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_09);
        builder.setBolt("BM10-Bolt", new StateChangeDetectionBolt(10), 1).setNumTasks(1).globalGrouping(
                "file-input-spout", Constants.Streams.STREAM_BM_10);
        // phase 2
        builder.setBolt("BM58-Bolt", new CombinedStateChangeDetectionBolt(5, 8), 1).setNumTasks(1).globalGrouping("BM05-Bolt", Util.getEdgeStreamId(5)).
                globalGrouping("BM08-Bolt", Util.getEdgeStreamId(8));
        builder.setBolt("BM69-Bolt", new CombinedStateChangeDetectionBolt(6, 9), 1).setNumTasks(1).globalGrouping("BM06-Bolt", Util.getEdgeStreamId(6)).
                globalGrouping("BM09-Bolt", Util.getEdgeStreamId(9));
        builder.setBolt("BM710-Bolt", new CombinedStateChangeDetectionBolt(7, 10), 1).setNumTasks(1).globalGrouping("BM07-Bolt", Util.getEdgeStreamId(7)).
                globalGrouping("BM10-Bolt", Util.getEdgeStreamId(10));
        // phase 3 - Monitoring
        builder.setBolt("Monitoring-58", new MonitoringBolt(Util.getCorrelatedChangeOfStateStreamId(5, 8)), 1).setNumTasks(1).
                globalGrouping("BM58-Bolt", Util.getCorrelatedChangeOfStateStreamId(5, 8));
        builder.setBolt("Monitoring-69", new MonitoringBolt(Util.getCorrelatedChangeOfStateStreamId(6, 9)), 1).setNumTasks(1).
                globalGrouping("BM69-Bolt", Util.getCorrelatedChangeOfStateStreamId(6, 9));
        builder.setBolt("Monitoring-710", new MonitoringBolt(Util.getCorrelatedChangeOfStateStreamId(7, 10)), 1).setNumTasks(1).
                globalGrouping("BM710-Bolt", Util.getCorrelatedChangeOfStateStreamId(7, 10));

        builder.setBolt("Report-Bolt", new ReportBolt(), 1).setNumTasks(1).
                globalGrouping("Monitoring-58", Util.getMonitoringStreamId(Util.getCorrelatedChangeOfStateStreamId(5, 8))).
                globalGrouping("Monitoring-69", Util.getMonitoringStreamId(Util.getCorrelatedChangeOfStateStreamId(6, 9))).
                globalGrouping("Monitoring-710", Util.getMonitoringStreamId(Util.getCorrelatedChangeOfStateStreamId(7, 10)));

        Config conf = new Config();
        int numberOfJobs = Integer.parseInt(args[0]);
        conf.put(Constants.INPUT_FILE_PATH, args[1]);
        conf.setNumAckers(0);
        //conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
        //conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
        //conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        //conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);

        if (args.length >= 3 && args[2].toLowerCase().equals("local")) {
            conf.setMaxTaskParallelism(20);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(args[0], conf, builder.createTopology());
            try {
                Thread.sleep(30 * 60 * 1000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
            cluster.shutdown();
        } else {
            conf.setNumWorkers(50 / numberOfJobs);
            try {
                for (int i = 0; i < numberOfJobs; i++) {
                    StormSubmitter.submitTopology("debs2012-job-" + i, conf, builder.createTopology());
                    System.out.println("Submitted job " + i);
                }
            } catch (AlreadyAliveException e) {
                LOGGER.error(e.getMessage(), e);
            } catch (InvalidTopologyException e) {
                LOGGER.error(e.getMessage(), e);
            } catch (AuthorizationException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
