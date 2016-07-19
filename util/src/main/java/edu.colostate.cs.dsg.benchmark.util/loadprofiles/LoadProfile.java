package edu.colostate.cs.dsg.benchmark.util.loadprofiles;

/**
 * Load profiles are used to simulate different input patterns for a
 * stream processing job.
 *
 * @author Thilina Buddhika
 */
public interface LoadProfile {
    /**
     * Decides whether to emit the next message
     * This method is invoked by the stream ingestion operator.
     *
     * @return @link{true} if the next message should be emitted
     */
    public boolean emit();

    /**
     * Returns Number of cycles completed, especially if the
     * load has a recurring pattern
     *
     * @return Number of cycles completed
     */
    public long getCycleCount();
}
