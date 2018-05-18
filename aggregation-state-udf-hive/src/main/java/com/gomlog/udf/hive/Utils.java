package com.gomlog.udf.hive;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;

import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;

class Utils {

    @Nonnull
    static BinaryObjectInspector asBinaryOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!"binary".equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be Binary: " + argOI.getTypeName());
        }
        return (BinaryObjectInspector) argOI;
    }

    static void mergeHll(HLLBuffer orig, byte[] otherData) {
        final HyperLogLog otherHLL = HyperLogLog.newInstance(Slices.wrappedBuffer(otherData));

        if (orig.hll == null) {
            orig.hll = otherHLL;
        } else {
            orig.hll.mergeWith(otherHLL);
        }
    }
}
