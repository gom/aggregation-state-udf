package com.gomlog.presto.udf;

import java.util.Set;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

public class ApproxDistinctStatePlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {

        return ImmutableSet.<Class<?>>builder()
                .add(ApproximateCountDistinctStateFunction.class)
                .build();
    }
}
