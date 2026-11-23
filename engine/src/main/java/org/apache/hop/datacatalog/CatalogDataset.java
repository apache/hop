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

package org.apache.hop.datacatalog;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * A dataset as a user names it in a transform or action: a table of a relational connection, a
 * file, or the id the catalog gives it. {@link #toRef} turns it into the {@link DatasetRef} a
 * catalog resolves. Experimental.
 */
@Getter
@Setter
public class CatalogDataset {
  private static final Class<?> PKG = CatalogDataset.class;

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

  /**
   * The dataset as Hop identifies it, so a catalog finds the dataset lineage wrote.
   *
   * @param variables to resolve the name, the connection and the schema
   * @param metadataProvider to load the connection of a table
   * @throws HopException when there is no name, the connection doesn't exist, or the dataset can't
   *     be named for a catalog (a table of a connection without a host)
   */
  public DatasetRef toRef(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException {
    String resolvedName = variables.resolve(name);
    if (StringUtils.isBlank(resolvedName)) {
      throw new HopException(BaseMessages.getString(PKG, "CatalogDataset.Error.NoName"));
    }
    CatalogDatasetType activeType = type == null ? CatalogDatasetType.TABLE : type;
    DatasetRef ref =
        switch (activeType) {
          case CATALOG_ID -> DatasetRef.ofCatalogId(resolvedName.trim());
          case FILE -> DatasetRef.ofFile(resolvedName, variables);
          case TABLE -> {
            String connectionName = variables.resolve(connection);
            DatabaseMeta databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, connectionName);
            if (databaseMeta == null) {
              throw new HopException(
                  BaseMessages.getString(
                      PKG, "CatalogDataset.Error.MissingConnection", connectionName, resolvedName));
            }
            yield DatasetRef.ofTable(
                databaseMeta, variables, variables.resolve(schema), resolvedName.trim());
          }
        };
    if (!ref.isComplete()) {
      // A connection without a host (an embedded database, a manual URL) has no name to look for
      throw new HopException(
          BaseMessages.getString(PKG, "CatalogDataset.Error.NoIdentity", resolvedName));
    }
    return ref;
  }
}
