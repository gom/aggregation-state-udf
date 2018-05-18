package com.gomlog.presto.udf;

import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;

import io.airlift.stats.cardinality.HyperLogLog;

public class HyperLogLogStateSerializer
        implements AccumulatorStateSerializer<HyperLogLogState>
{
    @Override
    public Type getSerializedType()
    {
        return HYPER_LOG_LOG;
    }

    @Override
    public void serialize(HyperLogLogState state, BlockBuilder out)
    {
        if (state.getHyperLogLog() == null) {
            out.appendNull();
        }
        else {
            HYPER_LOG_LOG.writeSlice(out, state.getHyperLogLog().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, HyperLogLogState state)
    {
        state.setHyperLogLog(HyperLogLog.newInstance(HYPER_LOG_LOG.getSlice(block, index)));
    }
}
