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
package org.apache.hop.pgvector.util;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Filters search results where a table column equals a value from an input stream field. */
@Getter
@Setter
public class PgVectorSearchFilter {

  @HopMetadataProperty(key = "column", injectionKey = "COLUMN")
  private String columnName;

  @HopMetadataProperty(key = "stream", injectionKey = "STREAM")
  private String streamField;

  public PgVectorSearchFilter() {}

  public PgVectorSearchFilter(String columnName, String streamField) {
    this.columnName = columnName;
    this.streamField = streamField;
  }
}
