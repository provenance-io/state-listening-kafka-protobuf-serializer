package io.provenance.statelistening.kafka.protobuf.serializer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigSubstitutionTest {

    @Test
    public void testConfigSubstitution() {
        Config conf = ConfigFactory.parseString(
                "app.kafka.streams {\n" +
                "input-topic-prefix = \"localhost\"\n" +
                "input-topic-pattern = ${app.kafka.streams.input-topic-prefix}\".+\"\n" +
                "}").resolve();

        assertEquals("localhost.+", conf.getString("app.kafka.streams.input-topic-pattern"));
    }
}
