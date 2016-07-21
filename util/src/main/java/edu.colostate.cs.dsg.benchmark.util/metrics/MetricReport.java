package edu.colostate.cs.dsg.benchmark.util.metrics;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author Thilina Buddhika
 */
public class MetricReport {

    private String messageId;
    protected int messageType;
    private String originEndpoint;
    private double[] metrics;

    public MetricReport(double[] metrics, String host) {
        this.messageType = 23790;
        this.originEndpoint = host;
        this.messageId = UUID.fromString(host).toString();
        this.metrics = metrics;
    }

    public byte[] marshall() throws IOException {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutStr = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        // write metadata
        dataOutStr.writeInt(this.messageType);
        dataOutStr.writeUTF(this.messageId);
        dataOutStr.writeUTF(this.originEndpoint);
        // write metrics
        dataOutStr.writeInt(metrics.length);
        for (double metric : metrics) {
            dataOutStr.writeDouble(metric);
        }
        dataOutStr.flush();
        byte[] marshalledBytes = baOutputStream.toByteArray();
        // cleanup
        baOutputStream.close();
        dataOutStr.close();

        return marshalledBytes;
    }
}
