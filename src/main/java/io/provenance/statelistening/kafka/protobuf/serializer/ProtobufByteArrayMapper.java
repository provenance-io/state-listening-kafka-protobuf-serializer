package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import tendermint.abci.Types;

/**
 * Transforms Protobuf raw message bytes into Protobuf {{Message}}s to be able
 * to easily register the Protobuf schema with the Confluent Schema Registry.
 */
public class ProtobufByteArrayMapper implements Transformer<byte[], byte[], KeyValue<byte[], Message>> {

    private static final String TOPIC_PREFIX = KafkaStreamsApplication.conf.getString(
            String.format("%s.%s", KafkaStreamsApplication.STREAMS_CONFIG_KEY, "input-topic-prefix"));

    private static final String BEGIN_BLOCK_REQ = "begin-block-req";
    private static final String BEGIN_BLOCK_RES = "begin-block-res";
    private static final String END_BLOCK_REQ = "end-block-req";
    private static final String END_BLOCK_RES = "end-block-res";

    private ProcessorContext context;

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<byte[], Message> transform(byte[] key, byte[] value) {
        try {
            Message msgValue;

            switch(getTopic()) {
                case BEGIN_BLOCK_REQ:
                    msgValue = Types.RequestBeginBlock.parseFrom(value);
                    break;
                case BEGIN_BLOCK_RES:
                    msgValue = Types.ResponseBeginBlock.parseFrom(value);
                    break;
                case END_BLOCK_REQ:
                    msgValue = Types.RequestEndBlock.parseFrom(value);
                    break;
                case END_BLOCK_RES:
                    msgValue = Types.ResponseEndBlock.parseFrom(value);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported topic");
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

    /**
     * Return the topic name for the current record, Removes any exiting topic prefix from the topic name.
     *
     * @return topic name.
     */
    private String getTopic() {
        String topic = context.topic();
        if (topic.startsWith(TOPIC_PREFIX)) {
            return topic.substring(TOPIC_PREFIX.length() + 1); // remove prefix + hyphen
        }
        return topic;
    }
}
