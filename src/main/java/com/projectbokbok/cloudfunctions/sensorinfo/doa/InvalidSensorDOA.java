package com.projectbokbok.cloudfunctions.sensorinfo.doa;

import com.google.cloud.bigquery.*;
import com.projectbokbok.cloudfunctions.sensorinfo.data.InvalidSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.exception.BokBokBigQueryException;
import com.projectbokbok.cloudfunctions.sensorinfo.util.BokHelperClass;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.TIME_FORMAT;

public class InvalidSensorDOA {
  private Table invalidSensorTable;
  private static final SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
  private static final Logger logger = Logger.getLogger(InvalidSensorDOA.class.getName());

  public InvalidSensorDOA(String projectId, String dataset, String tableName) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
      .build().getService();
    TableId tableId = TableId.of(dataset, tableName);
    this.invalidSensorTable = bigquery.getTable(tableId);
  }

  public void insertIndividualRecordToBigQueryTable(PubSubMessage pubSubMessage,  String errorMessageToStore) throws BokBokBigQueryException {
    InvalidSensorData invalidSensorData = new InvalidSensorData();
    invalidSensorData.setPayload(new String(Base64.getDecoder().decode(pubSubMessage.data)));
    invalidSensorData.setError(errorMessageToStore);
    invalidSensorData.setTimestamp(Timestamp.from(Instant.now()));

    InsertAllResponse response  = this.invalidSensorTable.insert(
      Collections.singletonList(InsertAllRequest.RowToInsert.of(invalidSensorData.toMap())),
      true, true);
    if (response.hasErrors()) {
      throw new BokBokBigQueryException("Issue inserting Big Query data for the Invalid table : " + BokHelperClass.getErrorFromInsertAllResponse(response));
    }
  }
}
