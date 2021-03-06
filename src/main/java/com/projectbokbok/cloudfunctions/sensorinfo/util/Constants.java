package com.projectbokbok.cloudfunctions.sensorinfo.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Constants {
  public final static String RAW_TABLE_NAME       = "raw";
  public final static String INVALID_TABLE_NAME   = "invalid";
  public final static String SENSOR_DATABASE      = "sensordata";
  public final static String SENSOR_DATABASE_TEST = "sensordatatest";
  public final static String PROJECT_ID           = "project-bok-bok";
  public final static String TIME_FORMAT        = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  public final static List<String> IGNORE_LIST        = new ArrayList<String>(Arrays.asList("BokArduino1-connected"));
}
