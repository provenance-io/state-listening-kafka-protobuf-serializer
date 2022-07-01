package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import tendermint.abci.Types;

public class ProtobufByteArrayMapper implements Transformer<byte[], byte[], KeyValue<byte[], Message>> {

    private ProcessorContext context;

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<byte[], Message> transform(byte[] key, byte[] value) {
        try {
            Message msgValue;
            String topic = context.topic();

            switch(topic) {
                case "localnet-begin-block-req":
                    msgValue = Types.RequestBeginBlock.parseFrom(value);
                    break;
                case "localnet-begin-block-res":
                    msgValue = Types.ResponseBeginBlock.parseFrom(value);
                    break;
                case "localnet-end-block-req":
                    msgValue = Types.RequestEndBlock.parseFrom(value);
                    break;
                case "localnet-end-block-res":
                    msgValue = Types.ResponseEndBlock.parseFrom(value);
                    break;
                default:
                    throw new IllegalStateException("Unsupported topic");
            }

            return new KeyValue<>(key, msgValue);
        } catch (Exception e) {
            throw new ProtobufByteArrayMapperException(e);
        }
    }

    @Override
    public void close() {
        // noop
    }
}
