package com.projectbokbok.cloudfunctions.sensorinfo;

import com.google.cloud.bigquery.*;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.gson.Gson;
import com.projectbokbok.cloudfunctions.sensorinfo.data.InvalidSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.data.RawSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.util.BokHelperClass;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.*;

public class ProcessSensorDataDriver implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(ProcessSensorDataDriver.class.getName());
  private SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT_1);

  @Override
  public void accept(PubSubMessage message, Context context) {
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(PROJECT_ID)
      .build().getService();
    TableId tableId = TableId.of(SENSOR_DATABASE, RAW_TABLE_NAME);
    try {
      if (message.data == null) {
        throw new Exception("Incoming data is null");
      }
      String data = new String(Base64.getDecoder().decode(message.data));
      Gson gson = BokHelperClass.gsonCreator();
      RawSensorData rawSensorData = gson.fromJson(data, RawSensorData.class);
      InsertAllResponse response = bigquery.insertAll(
        InsertAllRequest.newBuilder(tableId).addRow(rawSensorData.toMap()).build()
      );

      logger.info("Printing Sensor data : " + rawSensorData.toString());
    } catch (Exception e) {
      tableId = TableId.of(SENSOR_DATABASE, INVALID_TABLE_NAME);
      InvalidSensorData invalidSensorData = BokHelperClass.generateInvalidSensorData(e, message);
      logger.info("Printing the error json : " + new Gson().toJson(invalidSensorData));
      InsertAllResponse response = bigquery.insertAll(
        InsertAllRequest.newBuilder(tableId).addRow(invalidSensorData.toMap()).build()
      );
    }
  }


}
