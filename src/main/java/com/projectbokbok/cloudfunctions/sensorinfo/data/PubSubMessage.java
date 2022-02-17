package com.projectbokbok.cloudfunctions.sensorinfo.data;

import java.util.Map;

public class PubSubMessage {
  public String data;
  public Map<String, String> attributes;
  public String messageId;
  public String publishTime;

  @Override
  public String toString() {
    return "PubSubMessage{" +
      "data='" + data + '\'' +
      ", attributes=" + attributes +
      ", messageId='" + messageId + '\'' +
      ", publishTime='" + publishTime + '\'' +
      '}';
  }
}