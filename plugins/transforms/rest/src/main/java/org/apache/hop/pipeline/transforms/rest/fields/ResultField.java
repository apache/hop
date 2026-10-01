/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  /**
   * When set, the response body is handed on as raw bytes in a Binary result field instead of being
   * decoded to a String. Decoding a binary payload to a String and back is lossy, so this is the
   * only way to retrieve a file, an image or any other non-text response intact (issue #3746).
   */
  @HopMetadataProperty(key = "binary", injectionKey = "RESULT_BINARY")
  private boolean binary;

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

  public boolean isBinary() {
    return binary;
  }

  public void setBinary(boolean binary) {
    this.binary = binary;
  }
}
