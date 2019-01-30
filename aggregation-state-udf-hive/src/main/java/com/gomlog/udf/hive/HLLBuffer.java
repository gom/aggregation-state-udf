package com.gomlog.udf.hive;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationType;

import io.airlift.stats.cardinality.HyperLogLog;

@AggregationType(estimable = true)
final class HLLBuffer extends AbstractAggregationBuffer {
    private static final int DEFAULT_BUCKET_SIZE = 4096;

    HyperLogLog hll;

    HLLBuffer() {
        this.hll = HyperLogLog.newInstance(DEFAULT_BUCKET_SIZE);
    }

    @Override
    public int estimate() {
        return (hll == null) ? 0 : hll.estimatedInMemorySize();
    }

    void reset() {
        this.hll = HyperLogLog.newInstance(DEFAULT_BUCKET_SIZE);
    }
}
