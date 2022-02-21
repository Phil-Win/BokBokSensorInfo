package com.projectbokbok.cloudfunctions.sensorinfo.doa;

import com.google.cloud.bigquery.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.data.RawSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.exception.BokBokBigQueryException;
import com.projectbokbok.cloudfunctions.sensorinfo.util.BokHelperClass;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.TIME_FORMAT;

public class RawSensorDOA {
  private Table rawSensorTable;
  private static final SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);
  private static final Logger logger = Logger.getLogger(RawSensorDOA.class.getName());
  private Gson gson;

  public RawSensorDOA(String projectId, String dataset, String tableName) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
      .build().getService();
    TableId tableId = TableId.of(dataset, tableName);
    this.rawSensorTable = bigquery.getTable(tableId);
    this.gson  =  new GsonBuilder().setDateFormat(TIME_FORMAT).create();
  }

  public void insertIndividualRecordToBigQueryTable(PubSubMessage pubSubMessage) throws BokBokBigQueryException {
    InsertAllResponse response  = this.rawSensorTable.insert(
      Collections.singletonList(InsertAllRequest.RowToInsert.of(extractRawSensorDataFromPubSubMessage(pubSubMessage).toMap())),
      true, true);
    if (response.hasErrors()) {
      throw new BokBokBigQueryException("Issue inserting Big Query data for the Raw Sensor table : " + BokHelperClass.getErrorFromInsertAllResponse(response));
    }
  }

  private RawSensorData extractRawSensorDataFromPubSubMessage(PubSubMessage pubSubMessage) {
    String data = new String(Base64.getDecoder().decode(pubSubMessage.data));
    RawSensorData rawSensorData = gson.fromJson(data, RawSensorData.class);
    try {
      rawSensorData.setTimestamp(new Timestamp(sdf.parse(pubSubMessage.publishTime).getTime()));
    } catch (ParseException e ) {
      logger.info("Error parsing time : " + pubSubMessage.publishTime);
      rawSensorData.setTimestamp(new Timestamp(new Date().getTime()));
    }
    return rawSensorData;
  }


}
