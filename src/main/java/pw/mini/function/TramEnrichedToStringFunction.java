package pw.mini.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.json.JSONObject;
import pw.mini.TramEnriched;

public class TramEnrichedToStringFunction implements MapFunction<TramEnriched, String> {
    @Override
    public String map(TramEnriched value) throws Exception {
        JSONObject obj = TramToStringFunction.mapTramToJSON(value);
        obj.put("velocity", value.getVelocity());
        return obj.toString();
    }
}
