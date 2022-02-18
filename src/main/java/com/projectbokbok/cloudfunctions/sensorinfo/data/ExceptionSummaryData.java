package com.projectbokbok.cloudfunctions.sensorinfo.data;

public class ExceptionSummaryData {
  private int lineNumber;
  private String file;

  public ExceptionSummaryData(int lineNumber, String file) {
    this.lineNumber = lineNumber;
    this.file       = file;
  }

  public int getLineNumber() {
    return lineNumber;
  }

  public void setLineNumber(int lineNumber) {
    this.lineNumber = lineNumber;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  @Override
  public String toString() {
    return "ExceptionSummaryData{" +
      "lineNumber=" + lineNumber +
      ", file='" + file + '\'' +
      '}';
  }
}
