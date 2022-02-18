package com.projectbokbok.cloudfunctions.sensorinfo.util;

import com.google.cloud.bigquery.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.projectbokbok.cloudfunctions.sensorinfo.data.ExceptionSummaryData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.InvalidSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.exception.BokBokBigQueryException;

import java.sql.Timestamp;
import java.util.*;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.TIME_FORMAT;

public class BokHelperClass {

  public static Gson gsonCreator() {
    return new GsonBuilder().setDateFormat(TIME_FORMAT).create();
  }

  public static InvalidSensorData generateInvalidSensorData(String exception, Object originalMessage) {
    InvalidSensorData invalidSensorData = new InvalidSensorData();

    if (originalMessage.getClass().equals(PubSubMessage.class)) {
      try {
        invalidSensorData.setPayload(new String(Base64.getDecoder().decode(((PubSubMessage) originalMessage).data)));
      } catch (IllegalArgumentException e) {
        invalidSensorData.setPayload("Message a PubSubMessage but message not base 64 encoded, printing full message : " + originalMessage.toString());
      }
    } else {
      invalidSensorData.setPayload(originalMessage.toString());
    }
    invalidSensorData.setTimestamp(new Timestamp(new Date().getTime()));
    invalidSensorData.setError(exception);
    return invalidSensorData;
  }

  public static Table tableFinder(String projectId, String dataset, String tableName) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
      .build().getService();
    TableId tableId = TableId.of(dataset, tableName);
    return bigquery.getTable(tableId);
  }

  public static void insertIndividualRecordToBigQueryTable(String projectId, String dataset, String tableName, Map<String, Object> record) throws BokBokBigQueryException {
    Table table = tableFinder(projectId, dataset, tableName);
    InsertAllResponse response  = table.insert(
      Collections.singletonList(InsertAllRequest.RowToInsert.of(record)),
      true, true);
    if (response.hasErrors()) {
      StringBuilder error = new StringBuilder();
      error.append("Insert to invalid table in big query had errors : ").append(System.lineSeparator());
      for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
        error.append("Entry Number : ").append(entry.getKey());
        for (BigQueryError bigQueryError : entry.getValue()) {
          error.append(System.lineSeparator()).append(bigQueryError.getMessage());
        }
      }
      throw new BokBokBigQueryException(error.toString());
    }
  }
}

