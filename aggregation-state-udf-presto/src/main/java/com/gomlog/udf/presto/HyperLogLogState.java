package com.gomlog.udf.presto;

import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateMetadata;

import io.airlift.stats.cardinality.HyperLogLog;
import lombok.NonNull;

@AccumulatorStateMetadata(stateSerializerClass = HyperLogLogStateSerializer.class,
        stateFactoryClass = HyperLogLogStateFactory.class)
public interface HyperLogLogState extends AccumulatorState {
    @NonNull
    HyperLogLog getHyperLogLog();

    void setHyperLogLog(HyperLogLog value);

    void addMemoryUsage(int value);
}
