package com.projectbokbok.cloudfunctions.sensorinfo;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.data.RawSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.exception.BokBokBigQueryException;
import com.projectbokbok.cloudfunctions.sensorinfo.util.BokHelperClass;

import java.util.Base64;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.*;

public class ProcessSensorDataDriver implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(ProcessSensorDataDriver.class.getName());

  @Override
  public void accept(PubSubMessage message, Context context) {
    Gson gson = BokHelperClass.gsonCreator();
    try {
      String data = new String(Base64.getDecoder().decode(message.data));
      RawSensorData rawSensorData = gson.fromJson(data, RawSensorData.class);
      BokHelperClass.insertIndividualRecordToBigQueryTable(
        PROJECT_ID,
        SENSOR_DATABASE,
        RAW_TABLE_NAME,
        rawSensorData
        .toMap()
      );
    }catch (Exception e) {
      try {
        BokHelperClass.insertIndividualRecordToBigQueryTable(
          PROJECT_ID,
          SENSOR_DATABASE,
          INVALID_TABLE_NAME,
          BokHelperClass.generateInvalidSensorData(
            (e.getClass().equals(JsonSyntaxException.class) || e.getClass().equals(IllegalArgumentException.class)) ? e.getClass().getName() : e.getMessage(),
            message)
          .toMap()
        );
      } catch (BokBokBigQueryException bokBokBigQueryException) {
        logger.info("Exception thrown when trying to push error to invalid BQ table. Printing to logs");
        logger.info(bokBokBigQueryException.getMessage());
      }
    }
  }
}