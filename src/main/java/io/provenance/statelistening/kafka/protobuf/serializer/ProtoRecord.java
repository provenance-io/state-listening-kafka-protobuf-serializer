package io.provenance.statelistening.kafka.protobuf.serializer;

import com.google.protobuf.Message;

public class ProtoRecord {

    private final Message key;
    private final Message value;

    public ProtoRecord(Message key, Message value) {
        this.key = key;
        this.value = value;
    }

    public Message getKey() { return key; }
    public Message getValue() { return value; }
}
