package com.gomlog.udf.presto;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

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
import com.google.common.annotations.VisibleForTesting;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@AggregationFunction("approx_distinct_state")
@Description("Returns approx distinct state strings")
public final class ApproximateCountDistinctStateFunction {

    static final int DEFAULT_P = 15;
    static final int DEFAULT_SP = 25;

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.BIGINT) long value) {
        input(state, value, DEFAULT_P);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.INTEGER) int p) {
        input(state, value, p, DEFAULT_SP);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.INTEGER) int p,
                             @SqlType(StandardTypes.INTEGER) int sp) {
        HllBuffer hll = getOrCreateHyperLogLog(state, p, sp);
        state.addMemoryUsage(-hll.sizeof());
        hll.offer(value);
        state.addMemoryUsage(hll.sizeof());
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.DOUBLE) double value) {
        input(state, value, DEFAULT_P);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.DOUBLE) double value,
                             @SqlType(StandardTypes.INTEGER) int p) {
        input(state, Double.doubleToLongBits(value), p, DEFAULT_SP);
    }

    @InputFunction
    public static void input(@AggregationState HyperLogLogState state,
                             @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.INTEGER) int p,
                             @SqlType(StandardTypes.INTEGER) int sp) {
        input(state, Double.doubleToLongBits(value), p, sp);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value) {
        input(state, value, DEFAULT_P);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value,
                             @SqlType(StandardTypes.INTEGER) int p) {
        input(state, value, p, DEFAULT_SP);
    }

    @InputFunction
    @LiteralParameters("x")
    public static void input(@AggregationState HyperLogLogState state, @SqlType("varchar(x)") Slice value,
                             @SqlType(StandardTypes.INTEGER) int p, @SqlType(StandardTypes.INTEGER) int sp) {
        inputBinary(state, value, p, sp);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value) {
        inputBinary(state, value, DEFAULT_P);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value,
                                   @SqlType(StandardTypes.INTEGER) int p) {
        inputBinary(state, value, p, DEFAULT_SP);
    }

    @InputFunction
    public static void inputBinary(@AggregationState HyperLogLogState state,
                                   @SqlType(StandardTypes.VARBINARY) Slice value,
                                   @SqlType(StandardTypes.INTEGER) int p,
                                   @SqlType(StandardTypes.INTEGER) int sp) {
        HllBuffer hll = getOrCreateHyperLogLog(state, p, sp);
        state.addMemoryUsage(-hll.sizeof());
        hll.offer(value);
        state.addMemoryUsage(hll.sizeof());
    }

    static HllBuffer getOrCreateHyperLogLog(HyperLogLogState state, int p, int sp) {
        HllBuffer hll = state.getHyperLogLog();
        if (hll == null || hll.isEmpty()) {
            validateOptions(p, sp);
            hll = new HllBuffer(p, sp);
            state.setHyperLogLog(hll);
            state.addMemoryUsage(hll.sizeof());
        }
        return hll;
    }

    @VisibleForTesting
    static void validateOptions(int p, int sp) {
        if (p < 4 || p > sp && sp != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "p must be between 4 and sp (inclusive)");
        } else if (sp > 32) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "sp values greater than 32 not supported");
        }
    }

    @CombineFunction
    public static void combineState(@AggregationState HyperLogLogState state,
                                    @AggregationState HyperLogLogState otherState) {
        HllBuffer input = otherState.getHyperLogLog();

        HllBuffer previous = state.getHyperLogLog();
        if (previous == null) {
            state.setHyperLogLog(input);
            state.addMemoryUsage(input.sizeof());
        } else {
            state.addMemoryUsage(-previous.sizeof());
            try {
                previous.addAll(input);
                state.addMemoryUsage(previous.sizeof());
            } catch (CardinalityMergeException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage());
            }

        }
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void evaluateFinal(@AggregationState HyperLogLogState state, BlockBuilder out) {
        HllBuffer hll = state.getHyperLogLog();
        if (hll == null || hll.isEmpty()) {
            VARBINARY.writeSlice(out, Slices.allocate(0));
        } else {
            VARBINARY.writeSlice(out, hll.serialize());
        }
    }
}
