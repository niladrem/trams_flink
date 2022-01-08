package pw.mini;

import lombok.Data;

@Data
public class TramEnriched extends Tram {
    Double velocity;

    public TramEnriched(Tram tram, Double v) {
        this.lines = tram.lines;
        this.lon = tram.lon;
        this.vehicleNumber = tram.vehicleNumber;
        this.time = tram.time;
        this.lat = tram.lat;
        this.brigade = tram.brigade;
        this.velocity = v;
    }
}
