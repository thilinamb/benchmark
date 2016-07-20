package edu.colostate.cs.dsg.benchmark.healthstreams.thorax;

import edu.colostate.cs.dsg.benchmark.util.messaging.client.MessagingError;
import edu.colostate.cs.dsg.benchmark.util.messaging.client.SendUtility;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Thorax processing is implemented in this Bolt.
 * A single bolt can handle data for multiple patients.
 *
 * @author Thilina Buddhika
 */
public class ThoraxProcessorBolt extends BaseBasicBolt {

    private class PatientData {
        private double min;
        private double max;
        private double avg;
        private int count;
        private Queue<Double> lastTenMinsWindow = new ArrayDeque<>();

        public PatientData(double initialReading) {
            this.min = initialReading;
            this.max = initialReading;
            this.avg = initialReading;
            this.count = 1;
            lastTenMinsWindow.add(initialReading);
        }
    }

    private Logger logger;
    private Map<Integer, PatientData> patientDataMap;
    private Random random;
    private ByteBuffer buffer;
    private int counter;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        buffer = ByteBuffer.allocate(Long.BYTES);
        patientDataMap = new Hashtable<>();
        random = new Random(12);
        counter = 0;
        logger = Logger.getLogger(ThoraxProcessorBolt.class);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int patientId = tuple.getIntegerByField(Constants.FIELD_PATIENT_ID);
        double readingVal = tuple.getDoubleByField(Constants.FIELD_READING);
        // thorax calculation
        if (patientDataMap.containsKey(patientId)) {
            PatientData data = patientDataMap.get(patientId);
            if (data.min > readingVal) {
                data.min = readingVal;
            }
            if (data.max < readingVal) {
                data.max = readingVal;
            }
            data.avg = (data.avg * data.count + readingVal) / (data.count + 1);
            data.count++;
            data.lastTenMinsWindow.add(readingVal);
            if (data.lastTenMinsWindow.size() > 600) {
                data.lastTenMinsWindow.remove();
            }
        } else {
            PatientData data = new PatientData(readingVal);
            patientDataMap.put(patientId, data);
        }
        // sending an ack to calculate end-to-end latency periodically
        if (random.nextDouble() <= 0.00005) {
            long latency = tuple.getLongByField(Constants.FIELD_TS);
            buffer.putLong(latency);
            String sourceHostName = "lattice-" + tuple.getShortByField(Constants.SOURCE) + ":23456";
            try {
                SendUtility.sendBytes(sourceHostName, buffer.array());
            } catch (MessagingError e) {
                logger.error("Error sending latency data to the latency tracking server.", e);
            } finally {
                buffer.flip();
            }
        }
        if(++counter % 100000 == 0){
            logger.debug("Processed " + counter + " Messages.");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no outgoing streams
    }
}
