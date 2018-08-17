package com.gomlog.udf.presto;

import java.util.Set;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

public class ApproxDistinctStatePlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {

        return ImmutableSet.<Class<?>>builder()
                .add(ApproximateCountDistinctStateFunction.class)
                .add(ApproximateCountDistinctMergeFunction.class)
                .build();
    }
}
