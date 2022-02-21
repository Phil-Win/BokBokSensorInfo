package com.projectbokbok.cloudfunctions.sensorinfo.util;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class BokHelperClass {
  private static final Logger logger = Logger.getLogger(BokHelperClass.class.getName());

  public static String getErrorFromInsertAllResponse(InsertAllResponse insertAllResponse) {
    long beginTime = System.nanoTime();
    StringBuilder error = new StringBuilder();
    for (Map.Entry<Long, List<BigQueryError>> entry : insertAllResponse.getInsertErrors().entrySet()) {
      error.append("Entry Number : ").append(entry.getKey());
      for (BigQueryError bigQueryError : entry.getValue()) {
        error.append(System.lineSeparator()).append(bigQueryError.getMessage());
      }
    }
    return error.toString();
  }
}

