package com.projectbokbok.cloudfunctions.sensorinfo.util;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.projectbokbok.cloudfunctions.sensorinfo.data.ExceptionSummaryData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.InvalidSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;

import java.sql.Timestamp;
import java.util.Base64;
import java.util.Date;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.TIME_FORMAT;

public class BokHelperClass {

  public static Gson gsonCreator() {
    return new GsonBuilder().setDateFormat(TIME_FORMAT).create();
  }

  public static InvalidSensorData generateInvalidSensorData(Exception exception, PubSubMessage pubSubMessage) {
    InvalidSensorData invalidSensorData = new InvalidSensorData();
    invalidSensorData.setPayload(new String(Base64.getDecoder().decode(pubSubMessage.data)));
    invalidSensorData.setTimestamp(new Timestamp(new Date().getTime()));
    invalidSensorData.setError(
      new ExceptionSummaryData(
        exception.getStackTrace()[0].getLineNumber(),
        exception.getStackTrace()[0].getFileName())
        .toString()
    );
    return invalidSensorData;
  }

  public static Table tableFinder(String projectId, String dataset, String tableName) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
      .build().getService();
    TableId tableId = TableId.of(dataset, tableName);
    return bigquery.getTable(tableId);
  }
}
