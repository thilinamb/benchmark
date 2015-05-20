package edu.colostate.cs.dsg.benchmark.storm.util.window;

import java.util.ArrayDeque;
import java.util.ArrayList;

/**
 * Author: Thilina
 * Date: 5/20/15
 */
public class SlidingWindow {
    private ArrayDeque<SlidingWindowEntry> window = new ArrayDeque<SlidingWindowEntry>();
    private long tsStart;
    private long tsEnd;
    private long length;

    public SlidingWindow(long length) {
        this.length = length;
    }

    public void add(SlidingWindowEntry entry, SlidingWindowCallback callback) {
        //System.out.println("Adding " + entry.getTime());
        // very first entry in the window
        if (tsStart == 0l) {
            tsStart = entry.getTime();
        }
        // add the entry
        window.addLast(entry);
        // sliding window should be moved.
        if (entry.getTime() > tsEnd) {
            // update the timestamp end timestamp
            tsEnd = entry.getTime();
            // now we need to remove the entries which are expired
            long newTsStart = tsEnd - length + 1;
            ArrayList<SlidingWindowEntry> removed = new ArrayList<SlidingWindowEntry>();
            while (tsStart < newTsStart) {
                if (window.element().getTime() < newTsStart) {
                    removed.add(window.removeFirst());
                    tsStart = window.element().getTime();
                }
            }
            callback.remove(removed);
        }
        /*System.out.println("start:" + window.element().getTime() + "    end:" + window.getLast().getTime());
        System.out.print("{");
        for (SlidingWindowEntry e : window) {
            System.out.print(e.getTime() + ", ");
        }
        System.out.print("}\n"); */
    }

    public SlidingWindowEntry getOldestEntry(){
        return window.peekLast();
    }

}
