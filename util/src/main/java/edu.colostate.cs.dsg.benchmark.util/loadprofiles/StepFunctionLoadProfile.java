package edu.colostate.cs.dsg.benchmark.util.loadprofiles;

import java.util.Random;

/**
 * Simulates load profile that simulates a step function
 *
 * @author Thilina Buddhika
 */
public class StepFunctionLoadProfile implements LoadProfile {
    private int stepSize;
    private double[] steps;
    private long startTime;
    private Random random;

    public StepFunctionLoadProfile(int stepSize, int seed, double[] steps) {
        this.stepSize = stepSize;
        this.steps = steps;
        this.startTime = System.currentTimeMillis();
        this.random = new Random(seed);
    }

    @Override
    public boolean emit() {
        long now = System.currentTimeMillis();
        int stepIndex = (int) (Math.floor((double) (now - startTime)) / stepSize) % steps.length;
        return random.nextDouble() < steps[stepIndex];
    }

    @Override
    public long getCycleCount() {
        throw new UnsupportedOperationException("getCycleCount is not supported in StepFunctionLoadProfile yet.");
    }
}
