package com.gomlog.udf.presto;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

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
import io.airlift.stats.cardinality.HyperLogLog;

@AggregationFunction("approx_distinct_merge")
@Description("Returns cardinality of merging approx_distinct_state binary")
public final class ApproximateCountDistinctMergeFunction {

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value) {
        inputBinary(state, value);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value) {
        HyperLogLog input = HyperLogLog.newInstance(value);
        Utils.mergeHll(state, input);
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state,
                                    @AggregationState HyperLogLogState otherState) {
        HyperLogLog input = otherState.getHyperLogLog();
        Utils.mergeHll(state, input);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out) {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            BIGINT.writeLong(out, 0);
        } else {
            BIGINT.writeLong(out, hll.cardinality());
        }
    }
}
