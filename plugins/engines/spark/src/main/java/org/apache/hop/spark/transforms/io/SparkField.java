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

package org.apache.hop.spark.transforms.io;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Field definition for Spark File Input schema. */
@Getter
@Setter
public class SparkField implements Serializable {
  private static final long serialVersionUID = 1L;

  @HopMetadataProperty(key = "name")
  private String name;

  /** Hop type description, e.g. String, Integer, Number, Boolean, Date, BigNumber, Binary */
  @HopMetadataProperty(key = "type")
  private String hopType = "String";

  @HopMetadataProperty(key = "length")
  private int length = -1;

  @HopMetadataProperty(key = "precision")
  private int precision = -1;

  @HopMetadataProperty(key = "format")
  private String formatMask;

  public SparkField() {}

  public SparkField(String name, String hopType) {
    this.name = name;
    this.hopType = hopType;
  }

  public SparkField(String name, String hopType, int length, int precision) {
    this.name = name;
    this.hopType = hopType;
    this.length = length;
    this.precision = precision;
  }

  public IValueMeta createValueMeta() throws HopPluginException {
    int type = ValueMetaFactory.getIdForValueMeta(hopType);
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type, length, precision);
    if (formatMask != null && !formatMask.isEmpty()) {
      valueMeta.setConversionMask(formatMask);
    }
    return valueMeta;
  }
}
