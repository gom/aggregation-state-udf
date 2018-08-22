package com.gomlog.udf.presto;

import io.airlift.stats.cardinality.HyperLogLog;

public class Utils {

    static void mergeHll(HyperLogLogState state, HyperLogLog other) {
        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(other);
            state.addMemoryUsage(other.estimatedInMemorySize());
        } else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(other);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }
}
