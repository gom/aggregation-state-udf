package com.gomlog.udf.presto;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

import io.prestosql.spi.PrestoException;
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
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;

@AggregationFunction("approx_distinct_state")
@Description("Returns approx distinct state binary")
public final class ApproximateCountDistinctStateFunction {

    static final double DEFAULT_STANDARD_ERROR = 0.023;
    static final double LOWEST_MAX_STANDARD_ERROR = 0.0040625;
    static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.BIGINT) long value) {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.BIGINT) long value,
                             @SqlType(StandardTypes.DOUBLE) double maxStandardError) {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.DOUBLE) double value) {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.DOUBLE) double value,
                             @SqlType(StandardTypes.DOUBLE) double maxStandardError) {
        input(state, Double.doubleToLongBits(value), DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value) {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value,
                             @SqlType(StandardTypes.DOUBLE) double maxStandardError) {
        inputBinary(state, value, maxStandardError);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value) {
        inputBinary(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value,
                                   @SqlType(StandardTypes.DOUBLE) double maxStandardError) {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError) {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    static int standardErrorToBuckets(double maxStandardError) {
        if (!(maxStandardError >= LOWEST_MAX_STANDARD_ERROR
              && maxStandardError <= HIGHEST_MAX_STANDARD_ERROR)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                                      String.format("Max standard error must be in [%s, %s]: %s",
                                                    LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR,
                                                    maxStandardError));
        }
        return log2Ceiling((int) Math.ceil(1.0816 / (maxStandardError * maxStandardError)));
    }

    static int log2Ceiling(int value) {
        return Integer.highestOneBit(value - 1) << 1;
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state,
                                    @AggregationState HyperLogLogState otherState) {
        HyperLogLog input = otherState.getHyperLogLog();
        Utils.mergeHll(state, input);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out) {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            VARBINARY.writeSlice(out, Slices.EMPTY_SLICE);
        } else {
            VARBINARY.writeSlice(out, hll.serialize());
        }
    }
}
