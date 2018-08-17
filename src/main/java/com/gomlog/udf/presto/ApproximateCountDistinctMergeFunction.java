package com.gomlog.udf.presto;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import io.airlift.slice.Slice;

@AggregationFunction("approx_distinct_merge")
@Description("Returns cardinality of merging appro_distinct_state strings")
public final class ApproximateCountDistinctMergeFunction {

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value) {
        inputBinary(state, value);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value) {
        HllBuffer hll = state.getHyperLogLog();
        if (hll == null || hll.isEmpty()) {
            hll = new HllBuffer(value);
            state.setHyperLogLog(hll);
        } else {
            state.addMemoryUsage(-hll.sizeof());
            HllBuffer new_hll = new HllBuffer(value);
            mergeHll(hll, new_hll);
        }
        state.addMemoryUsage(hll.sizeof());
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state,
                                    @AggregationState HyperLogLogState otherState) {
        HllBuffer input = otherState.getHyperLogLog();

        HllBuffer previous = state.getHyperLogLog();
        if (previous == null || previous.isEmpty()) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.sizeof());
        } else {
            state.addMemoryUsage(-previous.sizeof());
            mergeHll(previous, input);
            state.addMemoryUsage(previous.sizeof());
        }
    }

    public static void mergeHll(HllBuffer current, HllBuffer other) {
        try {
            current.addAll(other);
        } catch (CardinalityMergeException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage());
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out) {
        HllBuffer hll = state.getHyperLogLog();
        if (hll == null || hll.isEmpty()) {
            BIGINT.writeLong(out, 0);
        } else {
            BIGINT.writeLong(out, hll.cardinality());
        }
    }
}
