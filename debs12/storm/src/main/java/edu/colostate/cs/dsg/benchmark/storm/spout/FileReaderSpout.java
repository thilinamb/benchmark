package edu.colostate.cs.dsg.benchmark.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import edu.colostate.cs.dsg.benchmark.storm.util.Constants;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class FileReaderSpout extends BaseRichSpout {

    private final static Logger LOGGER = Logger.getLogger(FileReaderSpout.class);

    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private long counter;
    private long lastTimestamp;
    private SpoutOutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.Streams.STREAM_BM_05,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.BM_VAL));
        outputFieldsDeclarer.declareStream(Constants.Streams.STREAM_BM_06,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.BM_VAL));
        outputFieldsDeclarer.declareStream(Constants.Streams.STREAM_BM_07,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.BM_VAL));
        outputFieldsDeclarer.declareStream(Constants.Streams.STREAM_BM_08,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.BM_VAL));
        outputFieldsDeclarer.declareStream(Constants.Streams.STREAM_BM_09,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.BM_VAL));
        outputFieldsDeclarer.declareStream(Constants.Streams.STREAM_BM_10,
                new Fields(Constants.DataFields.TIMESTAMP, Constants.DataFields.BM_VAL));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        String filePath = (String)map.get(Constants.INPUT_FILE_PATH);
        try {
            String machineName = InetAddress.getLocalHost().getHostName();
            bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
            bufferedWriter = new BufferedWriter(new FileWriter(new File("/tmp/storm-spout-" + machineName +
                    System.currentTimeMillis() + ".stat")));
        } catch (FileNotFoundException e) {
            LOGGER.error("Error opening the input file + " + filePath + " for reading.", e);
        } catch (UnknownHostException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void nextTuple() {
        String nextLine = null;
        counter++;
        try {
            nextLine = bufferedReader.readLine();
        } catch (IOException e) {
            LOGGER.error("Error reading from the file.", e);
        }
        if(nextLine != null){
            String[] fields = nextLine.split(",");
            long timeStamp = Long.parseLong(fields[0]);
            collector.emit(Constants.Streams.STREAM_BM_05, new Values(timeStamp, Integer.parseInt(fields[1])));
            collector.emit(Constants.Streams.STREAM_BM_06, new Values(timeStamp, Integer.parseInt(fields[2])));
            collector.emit(Constants.Streams.STREAM_BM_07, new Values(timeStamp, Integer.parseInt(fields[3])));
            collector.emit(Constants.Streams.STREAM_BM_08, new Values(timeStamp, Integer.parseInt(fields[4])));
            collector.emit(Constants.Streams.STREAM_BM_09, new Values(timeStamp, Integer.parseInt(fields[5])));
            collector.emit(Constants.Streams.STREAM_BM_10, new Values(timeStamp, Integer.parseInt(fields[6])));
        }
        if(lastTimestamp == 0){
            lastTimestamp = System.currentTimeMillis();
        }
        if(counter % 100000 == 0){
            long now = System.currentTimeMillis();
            try {
                bufferedWriter.write(((double) 100000 * 1000) / (now-lastTimestamp) +
                        "," + (((double)20 * 100000 * 8 * 1000) / ((1000 * 1000 * 1000) * (now-lastTimestamp))) + "\n");
                bufferedWriter.flush();
                lastTimestamp = now;
            } catch (IOException e) {
                LOGGER.error("Error writing statistics.", e);
            }
        }
    }
}
