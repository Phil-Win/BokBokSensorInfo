package com.projectbokbok.cloudfunctions.sensorinfo.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.projectbokbok.cloudfunctions.sensorinfo.data.InvalidSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Date;

public class BokHelperClass {

  public static Gson gsonCreator() {
    GsonBuilder gsonBuilder =  new GsonBuilder();
    gsonBuilder.registerTypeAdapter(Date.class, new GsonDateDeSerializer());
    return gsonBuilder.create();
  }

  public static InvalidSensorData generateInvalidSensorData(Exception exception, PubSubMessage pubSubMessage) {
    InvalidSensorData invalidSensorData = new InvalidSensorData();
    invalidSensorData.setPayload(new String(Base64.getDecoder().decode(pubSubMessage.data)));
    invalidSensorData.setTimestamp(new Timestamp(new Date().getTime()));
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    exception.printStackTrace(pw);
    invalidSensorData.setError(sw.toString());
    return invalidSensorData;
  }
}
