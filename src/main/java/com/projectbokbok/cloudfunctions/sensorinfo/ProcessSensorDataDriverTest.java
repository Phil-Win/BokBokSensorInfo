package com.projectbokbok.cloudfunctions.sensorinfo;

import com.google.cloud.bigquery.*;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.gson.Gson;
import com.projectbokbok.cloudfunctions.sensorinfo.data.InvalidSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.data.RawSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.util.BokHelperClass;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.*;

public class ProcessSensorDataDriverTest implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(ProcessSensorDataDriverTest.class.getName());

  @Override
  public void accept(PubSubMessage message, Context context) {
    Gson gson = BokHelperClass.gsonCreator();
    Table table     = BokHelperClass.tableFinder(PROJECT_ID, SENSOR_DATABASE_TEST, RAW_TABLE_NAME);
    try {
      if (message.data == null) {
        throw new Exception("Incoming data is null");
      }
      String data = new String(Base64.getDecoder().decode(message.data));
      RawSensorData rawSensorData = gson.fromJson(data, RawSensorData.class);
      InsertAllResponse response  = table.insert(
        Collections.singletonList(InsertAllRequest.RowToInsert.of(rawSensorData.toMap())),
        true, true);
      if (response.hasErrors()) {
        StringBuilder error = new StringBuilder();
        error.append("Insert to big query had errors : ").append(System.lineSeparator());
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet())  {
          error.append("Entry Number : ").append(entry.getKey());
          for (BigQueryError bigQueryError : entry.getValue()) {
            error.append(System.lineSeparator()).append(bigQueryError.getMessage());
          }
        }
        throw new Exception(error.toString());
      }
    } catch (Exception e) {
      table     = BokHelperClass.tableFinder(PROJECT_ID, SENSOR_DATABASE_TEST, INVALID_TABLE_NAME);
      InvalidSensorData invalidSensorData = BokHelperClass.generateInvalidSensorData(e, message);
      InsertAllResponse response  = table.insert(
        Collections.singletonList(InsertAllRequest.RowToInsert.of(invalidSensorData.toMap())),
        true, true);
      if (response.hasErrors()) {
        StringBuilder error = new StringBuilder();
        error.append("Insert to big query had errors : ").append(System.lineSeparator());
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet())  {
          error.append("Entry Number : ").append(entry.getKey());
          for (BigQueryError bigQueryError : entry.getValue()) {
            error.append(System.lineSeparator()).append(bigQueryError.getMessage());
          }
        }
        logger.info("ERROR While trying to push data to Invalid : " + error.toString());
      }
    }
  }
}