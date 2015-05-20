package edu.colostate.cs.dsg.benchmark.storm.util.window;

import java.util.List;

/**
 * Author: Thilina
 * Date: 5/20/15
 */
public interface SlidingWindowCallback {

    public void remove(List<SlidingWindowEntry> entries);

}
