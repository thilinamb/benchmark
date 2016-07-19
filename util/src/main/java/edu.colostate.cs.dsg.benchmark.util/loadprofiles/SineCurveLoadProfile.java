package edu.colostate.cs.dsg.benchmark.util.loadprofiles;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a load profile that simulates a sine curve
 *
 * @author Thilina Buddhika
 */
public class SineCurveLoadProfile implements LoadProfile {

    private int timeSlice;  // How many milliseconds correspond to one degree
    private long startTimestamp;
    private Random random;
    private AtomicLong cycles;

    public SineCurveLoadProfile(int timeSlice, int seed) {
        this.timeSlice = timeSlice;
        this.random = new Random(seed);
        this.startTimestamp = System.currentTimeMillis();
        this.cycles = new AtomicLong(0);
    }

    @Override
    public boolean emit() {
        long timeFromStart = System.currentTimeMillis() - startTimestamp;
        cycles.set((timeFromStart / timeSlice) / 360);
        double angleInRadians = (timeFromStart / timeSlice % 360) * Math.PI / 180;
        double probabilityOfDropping = 0.5 * (1 - Math.sin(angleInRadians));
        return random.nextDouble() > probabilityOfDropping;
    }

    public long getCycleCount() {
        return cycles.get();
    }
}
