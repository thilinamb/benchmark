package edu.colostate.cs.dsg.benchmark.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.*;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class FileReaderSpout extends BaseRichSpout {

    private final static Logger LOGGER = Logger.getLogger(FileReaderSpout.class);

    private BufferedReader bufferedReader;
    private SpoutOutputCollector collector;
    private int tupleCount = 0;


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
            bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
        } catch (FileNotFoundException e) {
            LOGGER.error("Error opening the input file + " + filePath + " for reading.", e);
        }
    }

    public void nextTuple() {
        String nextLine = null;
        try {
            nextLine = bufferedReader.readLine();
            tupleCount++;
            if(tupleCount % 100000 == 0){
                System.out.println("Processed " + (double)tupleCount/77576214 + " of tuples.");
            }
        } catch (IOException e) {
            LOGGER.error("Error reading from the file.", e);
        }
        if(nextLine != null){
            String[] fields = parse(nextLine);
            long timeStamp = new DateTime(fields[0]).getMillis();
            collector.emit(Constants.Streams.STREAM_BM_05, new Values(timeStamp, Integer.parseInt(fields[1])));
            collector.emit(Constants.Streams.STREAM_BM_06, new Values(timeStamp, Integer.parseInt(fields[2])));
            collector.emit(Constants.Streams.STREAM_BM_07, new Values(timeStamp, Integer.parseInt(fields[3])));
            collector.emit(Constants.Streams.STREAM_BM_08, new Values(timeStamp, Integer.parseInt(fields[4])));
            collector.emit(Constants.Streams.STREAM_BM_09, new Values(timeStamp, Integer.parseInt(fields[5])));
            collector.emit(Constants.Streams.STREAM_BM_10, new Values(timeStamp, Integer.parseInt(fields[6])));
        } else {
            System.out.println("Finished reading the input!");
        }
    }

    private String[] parse(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line);
        int index = 0;
        int arrayIndex = 0;
        String[] tokens = new String[7];
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (index == 0 || (index >= 12 && index <= 17)) {
                tokens[arrayIndex] = token;
                arrayIndex++;
            }
            if(index > 17){
                break;
            }
            index++;
        }
        return tokens;
    }

}
