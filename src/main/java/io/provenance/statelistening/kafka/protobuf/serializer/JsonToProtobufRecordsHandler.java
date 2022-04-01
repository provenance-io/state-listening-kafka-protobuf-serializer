package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import cosmos.base.store.v1beta1.Listening;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.curioswitch.common.protobuf.json.MessageMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tendermint.abci.Types;

public class JsonToProtobufRecordsHandler implements ConsumerRecordsHandler<byte[], byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(JsonToProtobufRecordsHandler.class);
    private static final MessageMarshaller marshaller = MessageMarshaller.builder()
            .register(Types.RequestBeginBlock.getDefaultInstance())
            .register(Types.ResponseBeginBlock.getDefaultInstance())
            .register(Types.RequestEndBlock.getDefaultInstance())
            .register(Types.ResponseEndBlock.getDefaultInstance())
            .register(Types.RequestDeliverTx.getDefaultInstance())
            .register(Types.ResponseDeliverTx.getDefaultInstance())
            .register(Listening.StoreKVPair.getDefaultInstance())
            .register(MsgKey.getDefaultInstance())
            .build();
    private final Producer<Message, Message> producer;
    private final String topicPrefix = "proto-";

    public JsonToProtobufRecordsHandler(Producer<Message, Message> producer) {
        this.producer = producer;
    }

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

    ProtoRecord unmarshall(ConsumerRecord<byte[], byte[]> record) {
        try {
            final MsgKey.Builder keyBuilder = MsgKey.newBuilder();
            marshaller.mergeValue(record.key(), keyBuilder);
            final MsgKey msgKey = keyBuilder.build();
            final MsgKey.EventType eventType = msgKey.getEventType();
            // all 'MsgKey.Event' have the same schema for STATE_CHANGE types
            final EventType eType = eventType.getNumber() == 2
                    ? EventType.valueOf(eventType.toString())
                    : EventType.valueOf(msgKey.getEvent()+"_"+msgKey.getEventType());
            Message.Builder valueBuilder;

            switch(eType) {
                case BEGIN_BLOCK_REQUEST:
                    valueBuilder = Types.RequestBeginBlock.newBuilder();
                    break;
                case BEGIN_BLOCK_RESPONSE:
                    valueBuilder = Types.ResponseBeginBlock.newBuilder();
                    break;
                case END_BLOCK_REQUEST:
                    valueBuilder = Types.RequestEndBlock.newBuilder();
                    break;
                case END_BLOCK_RESPONSE:
                    valueBuilder = Types.ResponseEndBlock.newBuilder();
                    break;
                case DELIVER_TX_REQUEST:
                    valueBuilder = Types.RequestDeliverTx.newBuilder();
                    break;
                case DELIVER_TX_RESPONSE:
                    valueBuilder = Types.ResponseDeliverTx.newBuilder();
                    break;
                case STATE_CHANGE:
                    valueBuilder = Listening.StoreKVPair.newBuilder();
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + eventType);
            }

            JsonFormat.parser().merge(new String(record.value()), valueBuilder);
            marshaller.mergeValue(record.value(), valueBuilder);
            return new ProtoRecord(msgKey, valueBuilder.build());
        } catch (Exception e) {
            throw new ConsumerRecordsHandlerException(e);
        }
    }

}