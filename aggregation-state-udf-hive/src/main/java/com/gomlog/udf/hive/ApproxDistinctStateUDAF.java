package com.gomlog.udf.hive;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.base.Preconditions;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@Description(name = "approx_distinct_state", value = "_FUNC_(expr x)"
                                                     + " - Returns an approximation of count(DISTINCT x) state using HyperLogLogPlus algorithm")
public final class ApproxDistinctStateUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(@Nonnull TypeInfo[] typeInfo)
            throws SemanticException {
        if (typeInfo.length != 1 && typeInfo.length != 2) {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                                               "_FUNC_ takes one or two arguments");
        }

        return new HLLEvaluator();
    }

    public static final class HLLEvaluator extends GenericUDAFEvaluator {

        @Nullable
        protected MapredContext mapredContext;

        @Override
        public final void configure(MapredContext mapredContext) {
            this.mapredContext = mapredContext;
        }

        private ObjectInspector origInputOI;
        private BinaryObjectInspector mergeInputOI;

        @Override
        public ObjectInspector init(@Nonnull Mode mode, @Nonnull ObjectInspector[] parameters)
                throws HiveException {
            assert (parameters.length == 1 || parameters.length == 2) : parameters.length;
            super.init(mode, parameters);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                this.origInputOI = parameters[0];
            } else {// from partial aggregation
                this.mergeInputOI = Utils.asBinaryOI(parameters[0]);
            }

            // initialize output
            final ObjectInspector outputOI;
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
            } else {// terminate
                outputOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
            }
            return outputOI;
        }

        @Override
        public HLLBuffer getNewAggregationBuffer() throws HiveException {
            return new HLLBuffer();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void reset(@Nonnull AggregationBuffer agg) throws HiveException {
            HLLBuffer buf = (HLLBuffer) agg;
            buf.hll = null;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void iterate(@Nonnull AggregationBuffer agg, @Nonnull Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }

            HLLBuffer buf = (HLLBuffer) agg;
            Object value = ObjectInspectorUtils.copyToStandardJavaObject(parameters[0], origInputOI);
            Preconditions.checkNotNull(buf.hll, HiveException.class);
            Slice data;
            if (value instanceof Integer) {
                data = Slices.wrappedIntArray((Integer) value);
            } else if (value instanceof Long) {
                data = Slices.wrappedLongArray((Long) value);
            } else if (value instanceof Short) {
                data = Slices.wrappedShortArray((Short) value);
            } else if (value instanceof Double) {
                data = Slices.wrappedDoubleArray((Double) value);
            } else if (value instanceof Float) {
                data = Slices.wrappedFloatArray((Float) value);
            } else if (value instanceof String) {
                data = Slices.utf8Slice((String) value);
            } else if (value instanceof byte[]) {
                data = Slices.wrappedBuffer((byte[]) value);
            } else {
                throw new HiveException(String.format("Cannot cast parameter: %s", value.getClass()));
            }
            buf.hll.add(data);
        }

        @SuppressWarnings("deprecation")
        @Override
        @Nullable
        public byte[] terminatePartial(@Nonnull AggregationBuffer agg) throws HiveException {
            HLLBuffer buf = (HLLBuffer) agg;
            if (buf.hll == null) {
                return null;
            }
            return buf.hll.serialize().getBytes();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void merge(@Nonnull AggregationBuffer agg, @Nullable Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            byte[] data = mergeInputOI.getPrimitiveJavaObject(partial);
            final HLLBuffer buf = (HLLBuffer) agg;
            Utils.mergeHll(buf, data);
        }

        @SuppressWarnings("deprecation")
        @Override
        public BytesWritable terminate(@Nonnull AggregationBuffer agg) throws HiveException {
            HLLBuffer buf = (HLLBuffer) agg;
            byte[] state = (buf.hll == null) ? new byte[0] : buf.hll.serialize().getBytes();
            return new BytesWritable(state);
        }

    }
}
