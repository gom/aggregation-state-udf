package com.gomlog.udf.presto;

import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import io.airlift.stats.cardinality.HyperLogLog;

public class HyperLogLogStateSerializer implements AccumulatorStateSerializer<HyperLogLogState> {
    @Override
    public Type getSerializedType() {
        return HYPER_LOG_LOG;
    }

    @Override
    public void serialize(HyperLogLogState state, BlockBuilder out) {
        if (state.getHyperLogLog() == null) {
            out.appendNull();
        } else {
            HYPER_LOG_LOG.writeSlice(out, state.getHyperLogLog().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, HyperLogLogState state) {
        state.setHyperLogLog(HyperLogLog.newInstance(HYPER_LOG_LOG.getSlice(block, index)));
    }
}
