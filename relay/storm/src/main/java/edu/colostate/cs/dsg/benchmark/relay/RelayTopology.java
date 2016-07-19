package edu.colostate.cs.dsg.benchmark.relay;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import org.apache.log4j.Logger;

/**
 * @author Thilina Buddhika
 */
public class RelayTopology {

    private static Logger LOGGER = Logger.getLogger(RelayTopology.class);

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sender", new SenderSpout(Integer.parseInt(args[2])), 1).setNumTasks(1);
        builder.setBolt("relay", new RelayBolt(), 1).globalGrouping("sender").setNumTasks(1);
        builder.setBolt("counter", new CounterBolt(), 1).globalGrouping("relay").setNumTasks(1);

        Config conf = new Config();
        conf.setNumAckers(0);
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);

        if(args.length >= 2 && args[1].toLowerCase().equals("local")) {
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(args[0], conf, builder.createTopology());
            try {
                Thread.sleep(30 * 60 * 1000);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
            cluster.shutdown();
        } else {
            conf.setNumWorkers(2);

            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                LOGGER.error(e.getMessage(), e);
            } catch (InvalidTopologyException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
