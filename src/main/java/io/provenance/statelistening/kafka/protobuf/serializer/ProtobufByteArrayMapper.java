package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;
import cosmos.base.store.v1beta1.Listening;
import network.cosmos.listening.plugins.kafka.service.MsgKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import tendermint.abci.Types;

public class ProtobufByteArrayMapper implements KeyValueMapper<byte[], byte[], KeyValue<Message, Message>> {

    private enum EventType {
        BEGIN_BLOCK_REQUEST,
        BEGIN_BLOCK_RESPONSE,
        END_BLOCK_REQUEST,
        END_BLOCK_RESPONSE,
        DELIVER_TX_REQUEST,
        DELIVER_TX_RESPONSE,
        STATE_CHANGE
    }

    @Override
    public KeyValue<Message, Message> apply(byte[] key, byte[] value) {
        try {
            final MsgKey msgKey = MsgKey.parseFrom(key);
            final MsgKey.EventType eventType = msgKey.getEventType();
            // no concatenation required for STATE_CHANGE event type
            final EventType eType = eventType.getNumber() == 2
                    ? EventType.valueOf(eventType.toString())
                    : EventType.valueOf(msgKey.getEvent()+"_"+msgKey.getEventType());
            Message msgValue;

            switch(eType) {
                case BEGIN_BLOCK_REQUEST:
                    msgValue = Types.RequestBeginBlock.parseFrom(value);
                    break;
                case BEGIN_BLOCK_RESPONSE:
                    msgValue = Types.ResponseBeginBlock.parseFrom(value);
                    break;
                case END_BLOCK_REQUEST:
                    msgValue = Types.RequestEndBlock.parseFrom(value);
                    break;
                case END_BLOCK_RESPONSE:
                    msgValue = Types.ResponseEndBlock.parseFrom(value);
                    break;
                case DELIVER_TX_REQUEST:
                    msgValue = Types.RequestDeliverTx.parseFrom(value);
                    break;
                case DELIVER_TX_RESPONSE:
                    msgValue = Types.ResponseDeliverTx.parseFrom(value);
                    break;
                case STATE_CHANGE:
                    msgValue = Listening.StoreKVPair.parseFrom(value);
                    break;
                default:
                    throw new IllegalStateException("Unexpected event type combo: " + eType);
            }

            return new KeyValue<>(msgKey, msgValue);
        } catch (Exception e) {
            throw new ProtobufByteArrayMapperException(e);
        }
    }
}
