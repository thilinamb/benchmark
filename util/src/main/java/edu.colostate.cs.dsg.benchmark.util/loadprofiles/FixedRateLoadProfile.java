package edu.colostate.cs.dsg.benchmark.util.loadprofiles;

import java.util.Random;

/**
 * @author Thilina Buddhika
 */
public class FixedRateLoadProfile implements LoadProfile {

    private final Random random = new Random(123);
    private final double threshold;

    public FixedRateLoadProfile(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean emit() {
        return random.nextDouble() <= threshold;
    }

    @Override
    public long getCycleCount() {
        return 0;
    }
}
