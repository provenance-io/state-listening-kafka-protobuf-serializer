package io.provenance.statelistening.kafka.protobuf.serializer;

import java.time.Duration;
import java.util.regex.Pattern;

import com.google.protobuf.Message;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.provenance.statelistening.kafka.protobuf.serializer.util.ConsumerSettings;
import io.provenance.statelistening.kafka.protobuf.serializer.util.ProducerSettings;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private volatile boolean keepConsuming = true;
    private final Consumer<byte[], byte[]> consumer;
    private final Producer<Message, Message> producer;
    private static final Config conf = ConfigFactory.load();

    public Application(final Consumer<byte[], byte[]> consumer,
                       final Producer<Message, Message> producer) {
        this.consumer = consumer;
        this.producer = producer;
    }

    public void runConsume(final ConsumerRecordsHandler<byte[], byte[]> recordsHandler) {
        try {
            final Pattern pattern = Pattern.compile(conf.getString("app.kafka.consumer.input-topic-pattern"));
            final Duration pollInterval = Duration.ofMillis(conf.getInt("app.kafka.consumer.poll-interval"));
            consumer.subscribe(pattern);
            while (keepConsuming) {
                final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(pollInterval);
                recordsHandler.process(consumerRecords);
            }
        } catch (ConsumerRecordsHandlerException e) {
            logger.error("Error: ", e);
        } finally {
            consumer.close();
            producer.close();
        }
    }

    public void shutdown() {
        keepConsuming = false;
    }

    public static void main(String[] args) {
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(ConsumerSettings.toProps(conf));
        final Producer<Message, Message> producer = new KafkaProducer<>(ProducerSettings.toProps(conf));
        final Application consumerApplication = new Application(consumer, producer);

        Runtime.getRuntime().addShutdownHook(new Thread(consumerApplication::shutdown));

        consumerApplication.runConsume(new JsonToProtobufRecordsHandler(producer));
    }

}