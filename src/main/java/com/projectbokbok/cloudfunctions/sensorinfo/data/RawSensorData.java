package com.projectbokbok.cloudfunctions.sensorinfo.data;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.TIME_FORMAT;


public class RawSensorData {
  private String device;
  private String sensor;
  private Timestamp timestamp;
  private String value;

  public String getDevice() {
    return device;
  }

  public void setDevice(String device) {
    this.device = device;
  }

  public String getSensor() {
    return sensor;
  }

  public void setSensor(String sensor) {
    this.sensor = sensor;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "RawSensorData{" +
      "device='" + device + '\'' +
      ", sensor='" + sensor + '\'' +
      ", timestamp=" + timestamp +
      ", value='" + value + '\'' +
      '}';
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TIME_FORMAT);
    map.put("device", device);
    map.put("sensor", sensor);
    map.put("timestamp", simpleDateFormat.format(timestamp));
    map.put("value", value);
    return map;
  }
}
