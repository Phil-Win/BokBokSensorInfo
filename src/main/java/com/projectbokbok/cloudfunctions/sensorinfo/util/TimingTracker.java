package com.projectbokbok.cloudfunctions.sensorinfo.util;

import java.util.ArrayList;
import java.util.List;

public class TimingTracker {
  private static TimingTracker INSTANCE;
  List<String> messages;

  private TimingTracker() {
    messages = new ArrayList<String>();
  }

  public static TimingTracker getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new TimingTracker();
    }
    return INSTANCE;
  }
  public void trackTiming(String newTrack) {
    messages.add(newTrack);
  }

  public String getTimings() {
    StringBuilder stringBuilder = new StringBuilder();
    for (String str : this.messages) {
      stringBuilder.append(str).append(System.lineSeparator());
    }
    return stringBuilder.toString();
  }
}
