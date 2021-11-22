package pw.mini.function;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.json.JSONObject;
import pw.mini.Tram;

@Slf4j
public class StringToTramFunction implements MapFunction<String, Tram> {
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public Tram map(String value) throws Exception {
        JSONObject json = new JSONObject(value);
        return Tram.builder()
            .brigade(json.getString("Brigade"))
            .lines(json.getString("Lines"))
            .vehicleNumber(json.getString("VehicleNumber"))
            .lon(json.getDouble("Lon"))
            .lat(json.getDouble("Lat"))
            .time(LocalDateTime.parse(json.getString("Time"), formatter))
            .build();
    }
}
