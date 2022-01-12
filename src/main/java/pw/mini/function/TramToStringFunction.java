package pw.mini.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.json.JSONObject;
import pw.mini.Tram;

import java.time.format.DateTimeFormatter;

public class TramToStringFunction implements MapFunction<Tram, String> {

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String map(Tram value) throws Exception {
        return mapTramToJSON(value).toString();
    }

    public static JSONObject mapTramToJSON(Tram value) {
        JSONObject obj = new JSONObject();
        obj.put("Lines", value.getLines());
        obj.put("Lon", value.getLon());
        obj.put("VehicleNumber", value.getVehicleNumber());
        obj.put("Time", value.getTime().format(formatter));
        obj.put("Lat", value.getLat());
        obj.put("Brigade", value.getBrigade());

        return obj;
    }
}
