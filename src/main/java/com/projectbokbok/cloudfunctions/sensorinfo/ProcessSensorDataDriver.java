package com.projectbokbok.cloudfunctions.sensorinfo;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.doa.InvalidSensorDOA;
import com.projectbokbok.cloudfunctions.sensorinfo.doa.RawSensorDOA;
import com.projectbokbok.cloudfunctions.sensorinfo.exception.BokBokBigQueryException;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.*;

public class ProcessSensorDataDriver implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(ProcessSensorDataDriver.class.getName());
  private static final SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);

  @Override
  public void accept(PubSubMessage message, Context context) {
    RawSensorDOA rawSensorDOA         = new RawSensorDOA(PROJECT_ID, SENSOR_DATABASE, RAW_TABLE_NAME);
    InvalidSensorDOA invalidSensorDOA = new InvalidSensorDOA(PROJECT_ID, SENSOR_DATABASE, INVALID_TABLE_NAME);
    if (IGNORE_LIST.contains(new String(Base64.getDecoder().decode(message.data)))) {
      return;
    }
    try {
      rawSensorDOA.insertIndividualRecordToBigQueryTable(message);
    }catch (Exception e) {
      try {
        invalidSensorDOA.insertIndividualRecordToBigQueryTable(message, e.getClass().getName());
      } catch (BokBokBigQueryException bokBokBigQueryException) {
        logger.info("Exception thrown when trying to push error to invalid BQ table. Printing to logs");
        logger.info(bokBokBigQueryException.getMessage());
      }
    }
  }
}