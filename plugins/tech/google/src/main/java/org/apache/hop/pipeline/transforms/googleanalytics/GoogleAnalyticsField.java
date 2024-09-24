/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hop.pipeline.transforms.googleanalytics;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class GoogleAnalyticsField {

  @HopMetadataProperty(key = "feed_field_type")
  private String feedFieldType;

  @HopMetadataProperty(key = "feed_field")
  private String feedField;

  @HopMetadataProperty(key = "output_field")
  private String outputFieldName;

  @HopMetadataProperty(key = "type")
  private String type;

  @HopMetadataProperty(key = "input_format")
  private String inputFormat;

  public GoogleAnalyticsField() {}

  public GoogleAnalyticsField(GoogleAnalyticsField f) {
    this.feedField = f.feedField;
    this.type = f.type;
    this.feedFieldType = f.feedFieldType;
    this.outputFieldName = f.outputFieldName;
    this.inputFormat = f.inputFormat;
  }

  public int getHopType() {
    return ValueMetaFactory.getIdForValueMeta(type);
  }

  public IValueMeta createValueMeta() throws HopPluginException {
    return ValueMetaFactory.createValueMeta(outputFieldName, getHopType());
  }

  public String getFeedFieldType() {
    return feedFieldType;
  }

  public void setFeedFieldType(String feedFieldType) {
    this.feedFieldType = feedFieldType;
  }

  public String getFeedField() {
    return feedField;
  }

  public void setFeedField(String feedField) {
    this.feedField = feedField;
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName(String outputFieldName) {
    this.outputFieldName = outputFieldName;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }
}
