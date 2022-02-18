package com.projectbokbok.cloudfunctions.sensorinfo;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.gson.Gson;
import com.projectbokbok.cloudfunctions.sensorinfo.data.PubSubMessage;
import com.projectbokbok.cloudfunctions.sensorinfo.data.RawSensorData;
import com.projectbokbok.cloudfunctions.sensorinfo.exception.BokBokBigQueryException;
import com.projectbokbok.cloudfunctions.sensorinfo.util.BokHelperClass;
import com.projectbokbok.cloudfunctions.sensorinfo.util.TimingTracker;

import java.util.Base64;
import java.util.logging.Logger;

import static com.projectbokbok.cloudfunctions.sensorinfo.util.Constants.*;

public class ProcessSensorDataDriverTest implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(ProcessSensorDataDriverTest.class.getName());

  @Override
  public void accept(PubSubMessage message, Context context) {
    TimingTracker timingTracker = TimingTracker.getInstance();
    long beginTime = System.nanoTime();
    Gson gson = BokHelperClass.gsonCreator();
    try {
      String data = new String(Base64.getDecoder().decode(message.data));
      RawSensorData rawSensorData = gson.fromJson(data, RawSensorData.class);
      BokHelperClass.insertIndividualRecordToBigQueryTable(
        PROJECT_ID,
        SENSOR_DATABASE_TEST,
        RAW_TABLE_NAME,
        rawSensorData
          .toMap()
      );
      timingTracker.trackTiming("accept Regular finish : " + (System.nanoTime() - beginTime));
    }catch (Exception e) {
      //logger.info("Error Message: ");
      timingTracker.trackTiming("accept Start Exception1 : " + (System.nanoTime() - beginTime));
      try {
        BokHelperClass.insertIndividualRecordToBigQueryTable(
          PROJECT_ID,
          SENSOR_DATABASE_TEST,
          INVALID_TABLE_NAME,
          BokHelperClass.generateInvalidSensorData(
            e.getClass().getName(),
            message)
            .toMap()
        );
        timingTracker.trackTiming("accept End Exception1 : " + (System.nanoTime() - beginTime));
      } catch (BokBokBigQueryException bokBokBigQueryException) {
        timingTracker.trackTiming("accept Start Exception2 : " + (System.nanoTime() - beginTime));
        logger.info("Exception thrown when trying to push error to invalid BQ table. Printing to logs");
        logger.info(bokBokBigQueryException.getMessage());
        timingTracker.trackTiming("accept End Exception2 : " + (System.nanoTime() - beginTime));
      }
    } finally {
    logger.info(timingTracker.getTimings());
    }
  }
}