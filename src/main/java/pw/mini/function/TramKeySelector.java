package pw.mini.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import pw.mini.Tram;

public class TramKeySelector implements KeySelector<Tram, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> getKey(Tram value) throws Exception {
        return Tuple2.of(value.getLines(), value.getBrigade());
    }
}
