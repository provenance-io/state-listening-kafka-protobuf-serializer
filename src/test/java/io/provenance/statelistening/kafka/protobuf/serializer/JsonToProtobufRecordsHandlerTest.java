package io.provenance.statelistening.kafka.protobuf.serializer;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class JsonToProtobufRecordsHandlerTest {

    private static final KafkaProtobufSerializer<Message> serde = new KafkaProtobufSerializer<>();
    private static final MockProducer<Message, Message> mockProducer = new MockProducer<>(true, serde, serde);
    private static final JsonToProtobufRecordsHandler recordsHandler = new JsonToProtobufRecordsHandler(mockProducer);

    private final byte[] beginBlockReqKey = getJson("begin-block-req-key.json");
    private final byte[] beginBlockReqValue = getJson("begin-block-req-value.json");
    private final byte[] beginBlockResKey = getJson("begin-block-res-key.json");
    private final byte[] beginBlockResValue = getJson("begin-block-res-value.json");
    private final byte[] endBlockReqKey = getJson("end-block-req-key.json");
    private final byte[] endBlockReqValue = getJson("end-block-req-value.json");
    private final byte[] endBlockResKey = getJson("end-block-res-key.json");
    private final byte[] endBlockResValue = getJson("end-block-res-value.json");
    private final byte[] deliverTxReqKey = getJson("deliver-tx-req-key.json");
    private final byte[] deliverTxReqValue = getJson("deliver-tx-req-value.json");
    private final byte[] deliverTxResKey = getJson("deliver-tx-res-key.json");
    private final byte[] deliverTxResValue = getJson("deliver-tx-res-value.json");
    private final byte[] stateChangeKey = getJson("state-change-key.json");
    private final byte[] stateChangeValue = getJson("state-change-value.json");
    private final ConsumerRecords<byte[], byte[]> consumerRecords = createConsumerRecords();

    public JsonToProtobufRecordsHandlerTest() throws URISyntaxException, IOException {
    }

    private byte[] getJson(String filename) throws URISyntaxException, IOException {
        final URL url = Objects.requireNonNull(
                JsonToProtobufRecordsHandlerTest.class.getClassLoader().getResource(filename));
        final Path path = Paths.get(url.toURI());
        return Files.readAllBytes(path);
    }

    private ConsumerRecords<byte[], byte[]> createConsumerRecords() {
        final String topic = "test";
        final int partition = 0;
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final List<ConsumerRecord<byte[], byte[]>> consumerRecordsList = new ArrayList<>();
//        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, beginBlockReqKey, beginBlockReqValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, beginBlockResKey, beginBlockResValue));
//        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, endBlockReqKey, endBlockReqValue));
//        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, endBlockResKey, endBlockResValue));
//        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, deliverTxReqKey, deliverTxReqValue));
//        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, deliverTxResKey, deliverTxResValue));
//        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, stateChangeKey, stateChangeValue));
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        recordsMap.put(topicPartition, consumerRecordsList);

        return new ConsumerRecords<>(recordsMap);
    }

    @Test
    public void testUnmarshall() {
        consumerRecords.forEach(recordsHandler::unmarshall);
        assertTrue(true);
    }

}