package com.projectbokbok.cloudfunctions.sensorinfo;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.projectbokbok.cloudfunctions.sensorinfo.data.RawSensorData;

public class Driver implements BackgroundFunction<Driver.PubSubMessage> {
  private static final Logger logger = Logger.getLogger(Driver.class.getName());
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  @Override
  public void accept(PubSubMessage message, Context context) {
    String data = message.data != null
      ? new String(Base64.getDecoder().decode(message.data))
      : "Hello, World";
    logger.info(data);
    RawSensorData rawSensorData = new Gson().fromJson(
      "{\"device\":\"ESP8266-1\",\"sensor\":\"waterLevelIndoor\",\"time\":\"2022-01-04\",\"value\":\"576\"}",
      RawSensorData.class
    );

    logger.info("Printing Sensor data device:sensor:time:value  - "+
      rawSensorData.getDevice()+
      rawSensorData.getSensor()+
      rawSensorData.getTimestamp()+
      rawSensorData.getValue()
    );
  }

  public static class PubSubMessage {
    String data;
    Map<String, String> attributes;
    String messageId;
    String publishTime;
  }
}
