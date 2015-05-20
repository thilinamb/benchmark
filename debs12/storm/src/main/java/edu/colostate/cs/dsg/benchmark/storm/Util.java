package edu.colostate.cs.dsg.benchmark.storm;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class Util {

    public static String getEdgeStreamId(int operatorId) {
        return "s" + operatorId + "-edge";
    }
}
