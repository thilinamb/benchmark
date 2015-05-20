package edu.colostate.cs.dsg.benchmark.storm;

/**
 * Author: Thilina
 * Date: 5/19/15
 */
public class Util {

    public static String getEdgeStreamId(int operatorId) {
        return "s" + operatorId + "-edge";
    }

    public static String getCorrelatedChangeOfStateStreamId(int inputStream1, int inputStream2) {
        return "s" + inputStream1 + inputStream2 + "-corr";
    }

    public static String getMonitoringStreamId(String inputStreamId){
        return inputStreamId.replace("corr", "monitoring");
    }
}
