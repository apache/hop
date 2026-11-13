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

package org.apache.hop.workflow.actions.checkdatacatalog;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

/** One dataset {@link ActionCheckDataCatalog} checks. */
@Getter
@Setter
public class CatalogDataset {

  @HopMetadataProperty(key = "type")
  private CatalogDatasetType type;

  /** The relational connection of a table. */
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  /** The schema of a table; empty for the connection's preferred schema. */
  @HopMetadataProperty(
      key = "schema",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_SCHEMA)
  private String schema;

  /** The table, the file name, or the catalog id. */
  @HopMetadataProperty(key = "name")
  private String name;

  public CatalogDataset() {
    type = CatalogDatasetType.TABLE;
  }

  public CatalogDataset(CatalogDatasetType type, String connection, String schema, String name) {
    this.type = type;
    this.connection = connection;
    this.schema = schema;
    this.name = name;
  }
}
