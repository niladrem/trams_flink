package pw.mini.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.json.JSONObject;
import pw.mini.Tram;

public class TramToStringFunction implements MapFunction<Tram, String> {

    @Override
    public String map(Tram value) throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("Lines", value.getLines());
        obj.put("Lon", value.getLon());
        obj.put("VehicleNumber", value.getVehicleNumber());
        obj.put("Time", value.getTime());
        obj.put("Lat", value.getLat());
        obj.put("Brigade", value.getBrigade());

        return obj.toString();
    }
}
