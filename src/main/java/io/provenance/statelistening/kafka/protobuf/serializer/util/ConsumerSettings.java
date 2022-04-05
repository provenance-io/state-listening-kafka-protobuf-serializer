package io.provenance.statelistening.kafka.protobuf.serializer.util;

import com.typesafe.config.Config;
import java.util.Properties;

/**
 * Kafka Consumer settings helper class.
 */
public final class ConsumerSettings {

    /** */
    private ConsumerSettings() {}

    private static final String CONSUMER_CONFIG_KEY = "app.kafka.consumer.kafka-clients";

    /**
     *
     * @param config consumer configuration properties
     * @return Properties
     */
    public static Properties toProps(Config config) {
        return Util.toProps(config.getConfig(CONSUMER_CONFIG_KEY));
    }

}
