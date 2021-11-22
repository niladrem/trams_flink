package pw.mini.function;

import java.time.LocalDateTime;
import lombok.val;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pw.mini.Tram;

public class DeduplicateProcessFunction extends KeyedProcessFunction<Tuple4<String, String, String, LocalDateTime>, Tram, Tram> implements
    CheckpointedFunction {

    private transient ValueState<Object> processedState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        val descriptor = new ValueStateDescriptor<>("processed",
            TypeInformation.of(Object.class));
        processedState = context.getKeyedStateStore().getState(descriptor);
    }

    @Override
    public void processElement(Tram value, Context ctx, Collector<Tram> out) throws Exception {
        val processed = processedState.value();

        if (processed == null) {
            processedState.update(new Object());
            out.collect(value);
        }
    }
}
