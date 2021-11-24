package pw.mini.function;

import java.time.LocalDateTime;

import lombok.val;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pw.mini.Tram;

public class DeduplicateProcessFunction extends KeyedProcessFunction<Tuple2<String, String>, Tram, Tram> implements
    CheckpointedFunction {

    private transient ValueState<LocalDateTime> lastDateTimeState;
    private transient MapState<LocalDateTime, Tuple2<Double, Double>> allDatesState;
    private transient OutputTag<Tram> zapierdala;

    private static Double earthR = 6371e3;

    public DeduplicateProcessFunction(OutputTag<Tram> zapierdala) {
        this.zapierdala = zapierdala;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        lastDateTimeState = context.getKeyedStateStore().getState(
                new ValueStateDescriptor<>("lastDateTime", TypeInformation.of(LocalDateTime.class)));
        allDatesState = context.getKeyedStateStore().getMapState(
                new MapStateDescriptor<>("dates", TypeInformation.of(LocalDateTime.class),
                        TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {})));
    }

    @Override
    public void processElement(Tram value, Context ctx, Collector<Tram> out) throws Exception {
        val currentTime = value.getTime();
        val lastDate = lastDateTimeState.value();

        if (!allDatesState.contains(currentTime)) {
            if (lastDate != null && currentTime.isAfter(lastDate)) {
                val previousValue = allDatesState.get(lastDate);
                val distanceInMeters = calculateDistance(previousValue.f0, previousValue.f1, value.getLat(), value.getLon());
                val timeDiff = currentTime.getSecond() - lastDate.getSecond();
                val v = 3.600 * distanceInMeters / timeDiff;
                if (v > 20.0) {
                    ctx.output(zapierdala, value);
                }

                lastDateTimeState.update(currentTime);
            }
            allDatesState.put(currentTime, new Tuple2<>(value.getLat(), value.getLon()));
            out.collect(value);
        }
    }

    private Double calculateDistance(Double lat1, Double lon1, Double lat2, Double lon2) {
        val fi1 = lat1 * Math.PI / 180;
        val fi2 = lat2 * Math.PI / 180;
        val deltaFi = (lat2 - lat1) * Math.PI / 180;
        val lambda = (lon2 - lon1) * Math.PI / 180;
        val haversine = Math.pow(Math.sin(deltaFi / 2), 2) + Math.cos(fi1) * Math.cos(fi2) * Math.pow(Math.sin(lambda / 2), 2);
        val c = 2 * Math.atan2(Math.sqrt(haversine), Math.sqrt(1 - haversine));
        return earthR * c;
    }
}
