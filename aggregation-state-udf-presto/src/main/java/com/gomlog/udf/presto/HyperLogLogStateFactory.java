package com.gomlog.udf.presto;

import static java.util.Objects.requireNonNull;

import org.openjdk.jol.info.ClassLayout;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.GroupedAccumulatorState;

import io.airlift.stats.cardinality.HyperLogLog;

public class HyperLogLogStateFactory implements AccumulatorStateFactory<HyperLogLogState> {
    @Override
    public HyperLogLogState createSingleState() {
        return new SingleHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getSingleStateClass() {
        return SingleHyperLogLogState.class;
    }

    @Override
    public HyperLogLogState createGroupedState() {
        return new GroupedHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getGroupedStateClass() {
        return GroupedHyperLogLogState.class;
    }

    public static class GroupedHyperLogLogState implements GroupedAccumulatorState, HyperLogLogState {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedHyperLogLogState.class)
                                                            .instanceSize();
        private final ObjectBigArray<HyperLogLog> hlls = new ObjectBigArray<>();
        private long size;
        private long groupId;

        @Override
        public final void setGroupId(long groupId) {
            this.groupId = groupId;
        }

        protected final long getGroupId() {
            return groupId;
        }

        @Override
        public void ensureCapacity(long size) {
            hlls.ensureCapacity(size);
        }

        @Override
        public HyperLogLog getHyperLogLog() {
            return hlls.get(getGroupId());
        }

        @Override
        public void setHyperLogLog(HyperLogLog value) {
            requireNonNull(value, "value is null");
            hlls.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value) {
            size += value;
        }

        @Override
        public long getEstimatedSize() {
            return INSTANCE_SIZE + size + hlls.sizeOf();
        }
    }

    public static class SingleHyperLogLogState implements HyperLogLogState {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleHyperLogLogState.class)
                                                            .instanceSize();
        private HyperLogLog hll;

        @Override
        public HyperLogLog getHyperLogLog() {
            return hll;
        }

        @Override
        public void setHyperLogLog(HyperLogLog value) {
            hll = value;
        }

        @Override
        public void addMemoryUsage(int value) {
            // No implementation
        }

        @Override
        public long getEstimatedSize() {
            long estimatedSize = INSTANCE_SIZE;
            if (hll != null) {
                estimatedSize += hll.estimatedInMemorySize();
            }
            return estimatedSize;
        }
    }
}
