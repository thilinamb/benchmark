package edu.colostate.cs.dsg.benchmark.util.metrics;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author Thilina Buddhika
 */
public class RegistrationMessage {
    private String messageId;
    protected int messageType;
    private String originEndpoint;
    private long ts;

    public RegistrationMessage(long ts, String host) {
        this.messageId = UUID.randomUUID().toString();
        this.messageType = 23789;
        this.originEndpoint = host;
        this.ts = ts;
    }

    public byte[] marshall() throws IOException {
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutStr = new DataOutputStream(new BufferedOutputStream(baOutputStream));
        // write metadata
        dataOutStr.writeInt(this.messageType);
        dataOutStr.writeUTF(this.messageId);
        dataOutStr.writeUTF(this.originEndpoint);
        // write ts
        dataOutStr.writeLong(this.ts);
        dataOutStr.flush();
        byte[] marshalledBytes = baOutputStream.toByteArray();
        // cleanup
        baOutputStream.close();
        dataOutStr.close();

        return marshalledBytes;
    }
}
