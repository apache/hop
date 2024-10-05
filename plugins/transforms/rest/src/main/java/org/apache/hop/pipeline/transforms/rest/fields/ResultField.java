package org.apache.hop.pipeline.transforms.rest.fields;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ResultField {

  @HopMetadataProperty(key = "name", injectionKey = "RESULT_FIELD_NAME")
  private String fieldName;

  @HopMetadataProperty(key = "code", injectionKey = "RESULT_CODE")
  private String code;

  @HopMetadataProperty(key = "response_time", injectionKey = "RESPONSE_TIME")
  private String responseTime;

  @HopMetadataProperty(key = "response_header", injectionKey = "RESPONSE_HEADER")
  private String responseHeader;

  public ResultField() {}

  public ResultField(String fieldName, String code, String responseTime, String responseHeader) {
    this.fieldName = fieldName;
    this.code = code;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getResponseTime() {
    return responseTime;
  }

  public void setResponseTime(String responseTime) {
    this.responseTime = responseTime;
  }

  public String getResponseHeader() {
    return responseHeader;
  }

  public void setResponseHeader(String responseHeader) {
    this.responseHeader = responseHeader;
  }
}
