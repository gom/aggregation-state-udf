package com.gomlog.presto.udf;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

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
import com.google.common.annotations.VisibleForTesting;

import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

@AggregationFunction("approx_distinct_state")
@Description("Returns approx distinct state strings")
public final class ApproximateCountDistinctStateFunction {

    static final double DEFAULT_STANDARD_ERROR = 0.023;
    static final double LOWEST_MAX_STANDARD_ERROR = 0.0040625;
    static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        input(state, Double.doubleToLongBits(value), maxStandardError);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value)
    {
        input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        inputBinary(state, value, maxStandardError);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.VARBINARY) Slice value)
    {
        inputBinary(state, value, DEFAULT_STANDARD_ERROR);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state, @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        HyperLogLog hll = getOrCreateHyperLogLog(state, maxStandardError);
        state.addMemoryUsage(-hll.estimatedInMemorySize());
        hll.add(value);
        state.addMemoryUsage(hll.estimatedInMemorySize());
    }

    static HyperLogLog getOrCreateHyperLogLog(HyperLogLogState state, double maxStandardError)
    {
        HyperLogLog hll = state.getHyperLogLog();
        if (hll == null) {
            hll = HyperLogLog.newInstance(standardErrorToBuckets(maxStandardError));
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.estimatedInMemorySize());
        }
        return hll;
    }

    @VisibleForTesting
    static int standardErrorToBuckets(double maxStandardError)
    {

        if(! (maxStandardError >= LOWEST_MAX_STANDARD_ERROR && maxStandardError <= HIGHEST_MAX_STANDARD_ERROR)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                                      String.format("Max standard error must be in [%s, %s]: %s",
                                                    LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR,
                                                    maxStandardError));
        }
        return log2Ceiling((int) Math.ceil(1.0816 / (maxStandardError * maxStandardError)));
    }

    static int log2Ceiling(int value)
    {
        return Integer.highestOneBit(value - 1) << 1;
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state, @AggregationState HyperLogLogState otherState)
    {
        HyperLogLog input = otherState.getHyperLogLog();

        HyperLogLog previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.estimatedInMemorySize());
        }
        else {
            state.addMemoryUsage(-previous.estimatedInMemorySize());
            previous.mergeWith(input);
            state.addMemoryUsage(previous.estimatedInMemorySize());
        }
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out)
    {
        HyperLogLog hyperLogLog = state.getHyperLogLog();
        if (hyperLogLog == null) {
            VARCHAR.writeString(out, "");
        }
        else {
            VARCHAR.writeSlice(out, hyperLogLog.serialize());
        }
    }
}
