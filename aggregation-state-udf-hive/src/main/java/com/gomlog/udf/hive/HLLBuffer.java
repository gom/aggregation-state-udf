package com.gomlog.udf.hive;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationType;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

@AggregationType(estimable = true)
final class HLLBuffer extends AbstractAggregationBuffer {
    private static final int DEFAULT_P = 15;
    private static final int DEFAULT_SP = 25;

    @Nullable
    HyperLogLogPlus hll;

    HLLBuffer() {
        this.hll = new HyperLogLogPlus.Builder(DEFAULT_P, DEFAULT_SP).build();
    }

    @Override
    public int estimate() {
        return (hll == null) ? 0 : hll.sizeof();
    }

    void reset(@Nonnegative int p, @Nonnegative int sp) {
        this.hll = new HyperLogLogPlus(p, sp);
    }

}
