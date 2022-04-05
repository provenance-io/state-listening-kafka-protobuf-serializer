package io.provenance.statelistening.kafka.protobuf.serializer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Kafka consumer records handler interface.
 *
 * @param <K> Message key deserializer type
 * @param <V> Message value deserializer type
 */
public interface ConsumerRecordsHandler<K, V> {

    /**
     * Process consumed records.
     *
     * @param consumerRecords Kafka messages
     * @throws ConsumerRecordsHandlerException when a process error occurs.
     */
    void process(ConsumerRecords<K, V> consumerRecords) throws ConsumerRecordsHandlerException;
}