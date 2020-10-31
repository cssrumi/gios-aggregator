package pl.airq.aggregator.integration;

import java.util.concurrent.atomic.AtomicInteger;

public class StationFactory {

    private static final AtomicInteger CURRENT_STATION = new AtomicInteger(0);

    public static String next() {
        CURRENT_STATION.incrementAndGet();
        return String.format("Station%d", CURRENT_STATION.get());
    }

}
