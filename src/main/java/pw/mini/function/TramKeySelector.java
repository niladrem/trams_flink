package pw.mini.function;

import java.time.LocalDateTime;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import pw.mini.Tram;

public class TramKeySelector implements KeySelector<Tram, Tuple4<String, String, String, LocalDateTime>> {

    @Override
    public Tuple4<String, String, String, LocalDateTime> getKey(Tram value) throws Exception {
        return Tuple4.of(value.getLines(), value.getVehicleNumber(), value.getBrigade(), value.getTime());
    }
}
