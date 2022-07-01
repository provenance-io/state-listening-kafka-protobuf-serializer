package io.provenance.statelistening.kafka.protobuf.serializer;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ProtobufRecordsHandlerTest {

    private static final KafkaProtobufSerializer<Message> serde = new KafkaProtobufSerializer<>();
    private static final MockProducer<Message, Message> mockProducer = new MockProducer<>(true, serde, serde);
    private static final ProtobufByteArrayMapper protobufByteArrayMapper = new ProtobufByteArrayMapper();
    private final byte[] beginBlockReqKey   = getResourceAsBytes("begin-block-req-key.pb");
    private final byte[] beginBlockReqValue = getResourceAsBytes("begin-block-req-value.pb");
    private final byte[] beginBlockResKey   = getResourceAsBytes("begin-block-res-key.pb");
    private final byte[] beginBlockResValue = getResourceAsBytes("begin-block-res-value.pb");
    private final byte[] endBlockReqKey     = getResourceAsBytes("end-block-req-key.pb");
    private final byte[] endBlockReqValue   = getResourceAsBytes("end-block-req-value.pb");
    private final byte[] endBlockResKey     = getResourceAsBytes("end-block-res-key.pb");
    private final byte[] endBlockResValue   = getResourceAsBytes("end-block-res-value.pb");
    private final byte[] deliverTxReqKey    = getResourceAsBytes("deliver-tx-req-key.pb");
    private final byte[] deliverTxReqValue  = getResourceAsBytes("deliver-tx-req-value.pb");
    private final byte[] deliverTxResKey    = getResourceAsBytes("deliver-tx-res-key.pb");
    private final byte[] deliverTxResValue  = getResourceAsBytes("deliver-tx-res-value.pb");
    private final byte[] stateChangeKey     = getResourceAsBytes("state-change-key.pb");
    private final byte[] stateChangeValue   = getResourceAsBytes("state-change-value.pb");
    private final ConsumerRecords<byte[], byte[]> consumerRecords = createConsumerRecords();

    public ProtobufRecordsHandlerTest() throws URISyntaxException, IOException {
    }

    private byte[] getResourceAsBytes(String filename) throws URISyntaxException, IOException {
        final URL url = Objects.requireNonNull(ProtobufRecordsHandlerTest.class.getClassLoader().getResource(filename));
        return Files.readAllBytes(Paths.get(url.toURI()));
    }

    private ConsumerRecords<byte[], byte[]> createConsumerRecords() {
        final String topic = "test";
        final int partition = 0;
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final List<ConsumerRecord<byte[], byte[]>> consumerRecordsList = new ArrayList<>();
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, beginBlockReqKey, beginBlockReqValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, beginBlockResKey, beginBlockResValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, endBlockReqKey, endBlockReqValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, endBlockResKey, endBlockResValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, deliverTxReqKey, deliverTxReqValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, deliverTxResKey, deliverTxResValue));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, stateChangeKey, stateChangeValue));
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        recordsMap.put(topicPartition, consumerRecordsList);

        return new ConsumerRecords<>(recordsMap);
    }

    @Test
    public void testProtobufByteArrayMapper() {
        consumerRecords.forEach(record -> protobufByteArrayMapper.transform(record.key(), record.value()));
        assertTrue(true);
    }

}