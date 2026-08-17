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

package org.apache.hop.spark.transforms.sql;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Optional override of the temporary view name used for one incoming transform. Without an override
 * the view name is derived from the transform name by {@link SparkSqlMeta#defaultViewName(String)}.
 */
@Getter
@Setter
public class SparkSqlView implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Name of the incoming (previous) transform whose Dataset is registered. */
  @HopMetadataProperty(key = "transform", injectionKey = "TRANSFORM")
  private String transformName;

  /** Temporary view name to register the Dataset under. Supports variables. */
  @HopMetadataProperty(key = "view", injectionKey = "VIEW")
  private String viewName;

  public SparkSqlView() {}

  public SparkSqlView(String transformName, String viewName) {
    this.transformName = transformName;
    this.viewName = viewName;
  }
}
