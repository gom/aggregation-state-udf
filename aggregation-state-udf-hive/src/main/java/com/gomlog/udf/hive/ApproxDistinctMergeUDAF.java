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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import com.google.common.base.Preconditions;

@Description(name = "approx_distinct_merge", value = "_FUNC_(expr x)"
                                                     + " - Returns results of merging aggregation state using HyperLogLogPlus algorithm")
public final class ApproxDistinctMergeUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(@Nonnull TypeInfo[] typeInfo) throws SemanticException {
        if (typeInfo.length != 1) {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                                               "_FUNC_ takes one argument");
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

        private BinaryObjectInspector inputOI;

        @Override
        public ObjectInspector init(@Nonnull Mode mode, @Nonnull ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1) : parameters.length;
            assert (parameters[0].getCategory() == Category.PRIMITIVE);
            super.init(mode, parameters);

            // initialize input
            this.inputOI = Utils.asBinaryOI(parameters[0]);

            // initialize output
            final ObjectInspector outputOI;
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
            } else {// terminate
                outputOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
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
            buf.reset();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void iterate(@Nonnull AggregationBuffer agg, @Nonnull Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }

            HLLBuffer buf = (HLLBuffer) agg;
            byte[] data = inputOI.getPrimitiveJavaObject(parameters[0]);
            Preconditions.checkNotNull(buf.hll, HiveException.class);
            Utils.mergeHll(buf, data);
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

            byte[] data = inputOI.getPrimitiveJavaObject(partial);
            final HLLBuffer buf = (HLLBuffer) agg;
            Utils.mergeHll(buf, data);
        }

        @SuppressWarnings("deprecation")
        @Override
        public LongWritable terminate(@Nonnull AggregationBuffer agg) throws HiveException {
            HLLBuffer buf = (HLLBuffer) agg;

            long cardinality = (buf.hll == null) ? 0L : buf.hll.cardinality();
            return new LongWritable(cardinality);
        }

    }
}
