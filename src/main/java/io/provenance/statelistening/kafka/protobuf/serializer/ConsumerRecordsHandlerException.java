package io.provenance.statelistening.kafka.protobuf.serializer;

/**
 * Exception handling class for processed consumer records.
 */
public class ConsumerRecordsHandlerException extends RuntimeException {
    /** */
    public ConsumerRecordsHandlerException(Throwable cause) { super(cause); }
}
