package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;
import cosmos.base.store.v1beta1.Listening;
import network.cosmos.listening.plugins.kafka.service.MsgKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tendermint.abci.Types;

/**
 * Converts Protobuf JSON binary data to Protobuf Message(s).
 */
public class ProtobufRecordsHandler implements ConsumerRecordsHandler<byte[], byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(ProtobufRecordsHandler.class);
    private final Producer<Message, Message> producer;
    private final String topicPrefix = "proto-";

    /** */
    public ProtobufRecordsHandler(Producer<Message, Message> producer) {
        this.producer = producer;
    }

    /**
     * Convert messages to protobuf messages and serialize to Kafka.
     *
     * @param consumerRecords Kafka messages
     * @throws ConsumerRecordsHandlerException is thrown when errors occur.
     */
    @Override
    public void process(ConsumerRecords<byte[], byte[]> consumerRecords) throws ConsumerRecordsHandlerException {
        consumerRecords.forEach(record -> {
            try {
                final String topic = topicPrefix + record.topic();
                final ProtoRecord protoRecord = unmarshall(record);
                producer.send(new ProducerRecord<>(topic, protoRecord.getKey(), protoRecord.getValue()),
                        (metadata, e) -> {
                            if(e != null) {
                                throw new ConsumerRecordsHandlerException(e);
                            } else {
                                logger.debug("produced record: topic:{} => partition:{}",
                                        metadata.topic(),
                                        metadata.partition());
                            }
                        });
            } catch (Exception e) {
                throw new ConsumerRecordsHandlerException(e);
            }
        });
    }

    private enum EventType {
        BEGIN_BLOCK_REQUEST,
        BEGIN_BLOCK_RESPONSE,
        END_BLOCK_REQUEST,
        END_BLOCK_RESPONSE,
        DELIVER_TX_REQUEST,
        DELIVER_TX_RESPONSE,
        STATE_CHANGE
    }

    /**
     * Unmarshall Protobuf JSON binary data to a Protobuf Message.
     *
     * @param record Kafka consumed record
     * @return ProtoRecord
     */
    ProtoRecord unmarshall(ConsumerRecord<byte[], byte[]> record) {
        try {
            final MsgKey key = MsgKey.parseFrom(record.key());
            final MsgKey.EventType eventType = key.getEventType();
            // no concatenation required for STATE_CHANGE event type
            final EventType eType = eventType.getNumber() == 2
                    ? EventType.valueOf(eventType.toString())
                    : EventType.valueOf(key.getEvent()+"_"+key.getEventType());
            Message value;

            switch(eType) {
                case BEGIN_BLOCK_REQUEST:
                    value = Types.RequestBeginBlock.parseFrom(record.value());
                    break;
                case BEGIN_BLOCK_RESPONSE:
                    value = Types.ResponseBeginBlock.parseFrom(record.value());
                    break;
                case END_BLOCK_REQUEST:
                    value = Types.RequestEndBlock.parseFrom(record.value());
                    break;
                case END_BLOCK_RESPONSE:
                    value = Types.ResponseEndBlock.parseFrom(record.value());
                    break;
                case DELIVER_TX_REQUEST:
                    value = Types.RequestDeliverTx.parseFrom(record.value());
                    break;
                case DELIVER_TX_RESPONSE:
                    value = Types.ResponseDeliverTx.parseFrom(record.value());
                    break;
                case STATE_CHANGE:
                    value = Listening.StoreKVPair.parseFrom(record.value());
                    break;
                default:
                    throw new IllegalStateException("Unexpected event type combo: " + eType);
            }

            return new ProtoRecord(key, value);
        } catch (Exception e) {
            throw new ConsumerRecordsHandlerException(e);
        }
    }

}