package com.gomlog.udf.presto;

import static io.prestosql.spi.type.BigintType.BIGINT;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

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
