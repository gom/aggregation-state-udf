package com.gomlog.udf.presto;

import static java.util.Objects.isNull;

import java.io.IOException;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

public class HllBuffer {
    private HyperLogLogPlus hll;

    public HllBuffer(int p, int sp) {
        this.hll = new HyperLogLogPlus.Builder(p, sp).build();
    }

    public HllBuffer(Slice serialized) {
        HyperLogLogPlus hll;
        try {
            hll = HyperLogLogPlus.Builder.build(serialized.getBytes());
        } catch (IOException e) {
            hll = null;
        }
        this.hll = hll;
    }

    public boolean isEmpty() {
        return isNull(this.hll);
    }

    public Slice serialize() {
        return serialize(this.hll);
    }

    public static Slice serialize(HyperLogLogPlus hll) {
        byte[] serialized;
        try {
            serialized = hll.getBytes();
        } catch (IOException e) {
            serialized = new byte[0];
        }
        DynamicSliceOutput out = new DynamicSliceOutput(serialized.length);
        out.writeBytes(serialized);
        return out.slice();
    }

    public int sizeof() {
        return hll.sizeof();
    }

    public boolean offer(Object obj) {
        return hll.offer(obj);
    }

    HyperLogLogPlus getHll() {
        return this.hll;
    }

    public void addAll(HllBuffer other) throws CardinalityMergeException {
        hll.addAll(other.getHll());
    }

    public long cardinality() {
        return hll.cardinality();
    }
}
