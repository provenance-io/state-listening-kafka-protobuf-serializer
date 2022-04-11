// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: proto/msg_key.proto

package network.cosmos.listening.plugins.kafka.service;

public interface MsgKeyOrBuilder extends
    // @@protoc_insertion_point(interface_extends:MsgKey)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>int64 block_height = 1 [json_name = "block_height"];</code>
   * @return The blockHeight.
   */
  long getBlockHeight();

  /**
   * <code>.MsgKey.Event event = 2;</code>
   * @return The enum numeric value on the wire for event.
   */
  int getEventValue();
  /**
   * <code>.MsgKey.Event event = 2;</code>
   * @return The event.
   */
  network.cosmos.listening.plugins.kafka.service.MsgKey.Event getEvent();

  /**
   * <code>int64 event_id = 3 [json_name = "event_id"];</code>
   * @return The eventId.
   */
  long getEventId();

  /**
   * <code>.MsgKey.EventType event_type = 4 [json_name = "event_type"];</code>
   * @return The enum numeric value on the wire for eventType.
   */
  int getEventTypeValue();
  /**
   * <code>.MsgKey.EventType event_type = 4 [json_name = "event_type"];</code>
   * @return The eventType.
   */
  network.cosmos.listening.plugins.kafka.service.MsgKey.EventType getEventType();

  /**
   * <code>int64 event_type_id = 5 [json_name = "event_type_id"];</code>
   * @return The eventTypeId.
   */
  long getEventTypeId();
}