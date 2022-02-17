package com.projectbokbok.cloudfunctions.sensorinfo.data;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class InvalidSensorData {
  private String payload;
  private Timestamp timestamp;
  private String error;

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "InvalidSensorData{" +
      "payload='" + payload + '\'' +
      ", timestamp=" + timestamp +
      ", error='" + error + '\'' +
      '}';
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("payload", payload);
    map.put("error", error);
    map.put("timestamp", timestamp);
    return map;
  }
}
