package io.provenance.statelistening.kafka.protobuf.serializer.util;

import com.typesafe.config.Config;

import java.util.Properties;

public final class ConsumerSettings {

    private ConsumerSettings() {}

    private static final String CONSUMER_CONFIG_KEY = "app.kafka.consumer.kafka-clients";

    public static Properties toProps(Config config) {
        return Util.toProps(config.getConfig(CONSUMER_CONFIG_KEY));
    }

}
