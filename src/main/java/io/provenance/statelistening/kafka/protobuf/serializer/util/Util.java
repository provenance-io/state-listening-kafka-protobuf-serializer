package io.provenance.statelistening.kafka.protobuf.serializer.util;

import com.typesafe.config.Config;

import java.util.Properties;

public final class Util {

    private Util() {}

    public static Properties toProps(final Config config) {
        Properties props = new Properties();
        config.entrySet().forEach(prop -> {
            Object value = prop.getValue().unwrapped();
            props.setProperty(prop.getKey(), value.toString());
        });
        return props;
    }
}
