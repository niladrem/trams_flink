package pw.mini;

import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Tram {
    String lines;
    Double lon;
    String vehicleNumber;
    LocalDateTime time;
    Double lat;
    String brigade;
}
