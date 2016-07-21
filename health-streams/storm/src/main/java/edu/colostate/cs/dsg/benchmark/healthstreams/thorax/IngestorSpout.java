package edu.colostate.cs.dsg.benchmark.healthstreams.thorax;

import edu.colostate.cs.dsg.benchmark.util.loadprofiles.FixedRateLoadProfile;
import edu.colostate.cs.dsg.benchmark.util.loadprofiles.LoadProfile;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Ingest ECG data from a file
 *
 * @author Thilina Buddhika
 */
public class IngestorSpout extends BaseRichSpout {

    private Logger logger;
    private String inputPath;
    private BufferedReader bufferedReader;
    private LoadProfile loadProfile;
    private SpoutOutputCollector collector;
    private short hostId;
    private int counter;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            logger = Logger.getLogger(IngestorSpout.class);
            this.inputPath = (String) map.get(Constants.INPUT_PATH);
            this.bufferedReader = getBufferedReaderForInput();
            this.loadProfile = new FixedRateLoadProfile(0.7);
            this.hostId = getHostId();
            this.collector = spoutOutputCollector;
            counter = 0;
        } catch (FileNotFoundException e) {
            logger.error("Error reading input file.", e);
        }
    }

    public void nextTuple() {
        if (loadProfile.emit()) {
            try {
                String line = bufferedReader.readLine();
                if (line == null) {
                    // continuously emit from the file.
                    bufferedReader.close();
                    bufferedReader = getBufferedReaderForInput();
                    line = bufferedReader.readLine();
                }
                String[] segments = line.split(",");
                collector.emit(Constants.STREAM_ECG, new Values(System.currentTimeMillis(),
                        Integer.parseInt(segments[0]), Double.parseDouble(segments[1]), hostId));
            } catch (IOException e) {
                logger.error("Error reading from the file.", e);
            }
            if (logger.isDebugEnabled() && ++counter % 100000 == 0) {
                logger.debug("Emitted " + counter + " messages.");
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.STREAM_ECG, new Fields(Constants.FIELD_TS,
                Constants.FIELD_PATIENT_ID, Constants.FIELD_READING, Constants.SOURCE));
    }

    private BufferedReader getBufferedReaderForInput() throws FileNotFoundException {
        return new BufferedReader(new FileReader(this.inputPath));
    }

    private short getHostId() {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        String hostName = inetAddr.getHostName();
        return Short.parseShort(hostName.substring(hostName.indexOf("-") + 1));
    }
}
