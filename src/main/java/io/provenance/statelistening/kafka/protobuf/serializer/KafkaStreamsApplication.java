package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

public class KafkaStreamsApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);
    private static final String STREAMS_CONFIG_KEY = "app.kafka.streams";
    private static final Config conf = ConfigFactory.load();

    private KafkaProtobufSerde<Message> protobufSerde(final boolean isKey) {
        final KafkaProtobufSerde<Message> protobufSerde = new KafkaProtobufSerde<>();
        final String schemaRegistryUrlKey = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        final String schemaRegistryUrl = String.format("%s.%s", STREAMS_CONFIG_KEY, schemaRegistryUrlKey);

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(schemaRegistryUrlKey, conf.getString(schemaRegistryUrl));
        protobufSerde.configure(serdeConfig, isKey);
        return protobufSerde;
    }

    private Topology buildTopology() {
        final Serde<byte[]> bytesSerde = Serdes.ByteArray();
        final String inputTopicFormat = String.format("%s.input-topic-pattern", STREAMS_CONFIG_KEY);
        final Pattern inputTopicPattern = Pattern.compile(conf.getString(inputTopicFormat));

        // dynamic output topic
        final TopicNameExtractor<Message, Message> topicNameExtractor =
                (key, value, recordContext) -> String.format("proto-%s", recordContext.topic());

        final StreamsBuilder builder = new StreamsBuilder();

        // input topic contains values in Protobuf binary without a registered schema
        final KStream<byte[], byte[]> protoStream =
                builder.stream(inputTopicPattern, Consumed.with(bytesSerde, bytesSerde));

        // unmarshal and write Protobuf data registering message schema
        protoStream
                .map(new ProtobufByteArrayMapper())
                .to(topicNameExtractor, Produced.with(protobufSerde(true), protobufSerde(false)));

        return builder.build();
    }

    private void run() {
        Properties streamProps = PropUtil.toProps(conf.getConfig(String.format("%s.config", STREAMS_CONFIG_KEY)));

        final Topology topology = this.buildTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        new KafkaStreamsApplication().run();
    }
}
