package flights.utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import radar.AirportUpdateEvent;

public class Utils {
    private static final Logger logger = LogManager.getLogger(Utils.class);

    public static final double AVERAGE_RADIUS_OF_EARTH_KM = 6371;
    public static final int AVERAGE_AIRSPEED = 900; // km per hour

    private static int getDistanceFromLatLonInKm(Double sourceLat, Double sourceLong, Double destLat, Double destLong) {
        double lngDistance = Math.toRadians(sourceLong - destLong);
        double latDistance = Math.toRadians(sourceLat - destLat);

        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(sourceLat))
                        * Math.cos(Math.toRadians(destLat))
                        * Math.sin(lngDistance / 2)
                        * Math.sin(lngDistance / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return (int) Math.round(AVERAGE_RADIUS_OF_EARTH_KM * c);
    }

    public static Long approximateArrivalTime(Long departureTime, Map<String, AirportUpdateEvent> destination) {
        int distance = Utils.getDistanceFromLatLonInKm(
                destination.get("start").getLatitude(),
                destination.get("start").getLongitude(),
                destination.get("end").getLatitude(),
                destination.get("end").getLongitude());

        logger.info(String.format(
                "Appx distance between %s and %s is %d",
                destination.get("start").getCity(), destination.get("end").getCity(), distance));
        Timestamp arrivalTime = new Timestamp(departureTime);
        BigDecimal duration = new BigDecimal((double) distance / AVERAGE_AIRSPEED);
        long hours = duration.intValue();
        long minutes = duration.subtract(new BigDecimal(hours))
                .multiply(new BigDecimal(60))
                .longValue();
        arrivalTime.setTime(departureTime + hours * 1000 * 60 * 60 + minutes * 1000 * 60);

        return arrivalTime.getTime();
    }
}
