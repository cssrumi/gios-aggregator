package pl.airq.aggregator.integration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.time.OffsetDateTime;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testcontainers.shaded.com.google.common.util.concurrent.AtomicDouble;
import pl.airq.common.domain.gios.Installation;

public class InstallationFactory {

    private static final AtomicDouble LAST_INSTALLATION_VALUE = new AtomicDouble(1);
    public static final String DEFAULT_STATION = "Station";

    public static Installation create(String station, Field field, Float value) {
        return installation(station, field, value, OffsetDateTime.now());
    }

    public static Installation create(String station, Field field, OffsetDateTime timestamp) {
        return installation(station, field, getNextValue(), timestamp);
    }

    public static Installation create(String station, Field field) {
        return installation(station, field, getNextValue(), OffsetDateTime.now());
    }

    public static Installation create(Field field) {
        return installation(DEFAULT_STATION, field, getNextValue(), OffsetDateTime.now());
    }

    private static Installation installation(String station, Field field, Float value, OffsetDateTime timestamp) {
        final Constructor<?>[] constructors = Installation.class.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
            if (Modifier.isPrivate(constructor.getModifiers())) {
                constructor.setAccessible(true);
            }

            Class<?>[] params = constructor.getParameterTypes();
            if (params.length != 7) {
                continue;
            }

            try {
                return (Installation) constructor.newInstance(
                        Long.valueOf(RandomStringUtils.randomNumeric(5)),
                        station,
                        timestamp,
                        value,
                        Float.valueOf(1.0f),
                        Float.valueOf(1.0f),
                        field.name());
            } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Unable to find Installation constructor");
    }

    private static Float getNextValue() {
        Float value = RandomUtils.nextFloat();
        while (value.equals(LAST_INSTALLATION_VALUE.floatValue())) {
            value = RandomUtils.nextFloat();
        }
        LAST_INSTALLATION_VALUE.set(value);

        return value;
    }

}
