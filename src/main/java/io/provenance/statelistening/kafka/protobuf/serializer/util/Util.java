package io.provenance.statelistening.kafka.protobuf.serializer.util;

import com.typesafe.config.Config;
import java.util.Properties;

/**
 * Utility class
 */
public final class Util {

    /** */
    private Util() {}

    /**
     * Convert typesafe configuration properties to {{Properties}}.
     *
     * @param config type safe {{Config}}
     * @return props
     */
    public static Properties toProps(final Config config) {
        Properties props = new Properties();
        config.entrySet().forEach(prop -> {
            Object value = prop.getValue().unwrapped();
            props.setProperty(prop.getKey(), value.toString());
        });
        return props;
    }
}
