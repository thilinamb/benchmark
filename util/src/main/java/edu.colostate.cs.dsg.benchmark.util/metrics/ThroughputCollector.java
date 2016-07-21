package edu.colostate.cs.dsg.benchmark.util.metrics;

import edu.colostate.cs.dsg.benchmark.util.messaging.client.MessagingError;
import edu.colostate.cs.dsg.benchmark.util.messaging.client.SendUtility;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Thilina Buddhika
 */
public class ThroughputCollector implements Runnable {

    private static final long REPORT_INTERVAL_MS = 2000;
    private static final String STAT_SERVER_URL = "lattice-97:12345";

    private static ThroughputCollector instance = new ThroughputCollector();

    private Logger logger = Logger.getLogger(ThroughputCollector.class);
    private List<ThroughputProfiler> computationList = new ArrayList<>();
    private long lastTs;
    private ScheduledExecutorService throughputReporter = Executors.newScheduledThreadPool(1);
    private String host;

    private ThroughputCollector() {
        // singleton: private constructor
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        this.host = inetAddr.getHostName();
    }

    public static ThroughputCollector getInstance() {
        return instance;
    }

    public synchronized void register(ThroughputProfiler profiler) {
        computationList.add(profiler);
        if (computationList.size() == 1) {
            RegistrationMessage register = new RegistrationMessage(System.currentTimeMillis(), host);
            try {
                SendUtility.sendBytes(STAT_SERVER_URL, register.marshall());
            } catch (MessagingError | IOException e) {
                logger.error("Error sending registration message.", e);
            }
            throughputReporter.scheduleAtFixedRate(this, REPORT_INTERVAL_MS, REPORT_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
            lastTs = System.currentTimeMillis();
        }
    }

    @Override
    public void run() {
        // calculate the cumulative throughput
        long totalMessageCount = 0;
        synchronized (this) {
            for (ThroughputProfiler profiler: computationList) {
                totalMessageCount += profiler.getNumberOfMessagesProcessedSinceLastInvocation();
            }
        }
        long tsNow = System.currentTimeMillis();
        double throughput = totalMessageCount * 1000/(tsNow - lastTs);
        lastTs = tsNow;

        // report this to a centralized stat-server
        // TODO: we are currently using a custom statics server and its supported message formats and endpoints
        double[] metrics = new double[]{throughput, -1, -1};
        MetricReport metricReport = new MetricReport(metrics, host);
        try {
            SendUtility.sendBytes(STAT_SERVER_URL, metricReport.marshall());
        } catch (MessagingError | IOException messagingError) {
            logger.error("Error reporting metrics.", messagingError);
        }
    }
}
