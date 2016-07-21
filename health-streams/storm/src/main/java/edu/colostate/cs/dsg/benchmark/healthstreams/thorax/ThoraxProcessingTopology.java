package edu.colostate.cs.dsg.benchmark.healthstreams.thorax;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author Thilina Buddhika
 */
public class ThoraxProcessingTopology {
    private static final Logger LOGGER = Logger.getLogger(ThoraxProcessingTopology.class);

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ingestor", new IngestorSpout(), 1).setNumTasks(1);
        builder.setBolt("processor", new ThoraxProcessorBolt(), 1).setNumTasks(1).globalGrouping("ingestor",
                Constants.STREAM_ECG);

        Config conf = new Config();
        // disabling ackers
        conf.setNumAckers(0);
        // number of jobs
        int numberOfJobs = Integer.parseInt(args[0]);
        // set the number of inputs
        if(args.length >= 3){
            String input = args[2];
            conf.put(Constants.INPUT_PATH, input);
        } else {
            conf.put(Constants.INPUT_PATH, Constants.DEFAULT_INPUT);
        }
        // set the cluster mode
        if (args.length >= 2 && args[1].toLowerCase().equals("local")) {
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
                    StormSubmitter.submitTopology("thorax-job-" + i, conf, builder.createTopology());
                    System.out.println("Submitted job " + i);
                }
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

}
