package io.provenance.statelistening.kafka.protobuf.serializer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerRecordsHandler<K, V> {
    void process(ConsumerRecords<K, V> consumerRecords) throws ConsumerRecordsHandlerException;
}