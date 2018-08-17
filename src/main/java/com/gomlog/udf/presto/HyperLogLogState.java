package com.gomlog.udf.presto;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import lombok.NonNull;

@AccumulatorStateMetadata(stateSerializerClass = HyperLogLogStateSerializer.class,
        stateFactoryClass = HyperLogLogStateFactory.class)
public interface HyperLogLogState extends AccumulatorState {
    @NonNull
    HllBuffer getHyperLogLog();

    void setHyperLogLog(HllBuffer value);

    void addMemoryUsage(int value);
}
