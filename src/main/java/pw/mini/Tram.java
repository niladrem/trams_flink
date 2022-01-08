package pw.mini;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Tram {
    String lines;
    Double lon;
    String vehicleNumber;
    LocalDateTime time;
    Double lat;
    String brigade;
}
