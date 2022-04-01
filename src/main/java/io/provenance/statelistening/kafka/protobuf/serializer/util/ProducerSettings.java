package io.provenance.statelistening.kafka.protobuf.serializer.util;

import com.typesafe.config.Config;

import java.util.Properties;

public final class ProducerSettings {

    private ProducerSettings() {}

    private static final String PRODUCER_CONFIG_KEY = "app.kafka.producer.kafka-clients";

    public static Properties toProps(Config config) {
        return Util.toProps(config.getConfig(PRODUCER_CONFIG_KEY));
    }

}
