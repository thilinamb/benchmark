package edu.colostate.cs.dsg.benchmark.util.metrics;

/**
 * Any computation that should be included in throughput profiling should
 * implement this interface and register itself with ThroughputCollector
 *
 * @author Thilina Buddhika
 */
public interface ThroughputProfiler {

    public long getNumberOfMessagesProcessedSinceLastInvocation();
}
