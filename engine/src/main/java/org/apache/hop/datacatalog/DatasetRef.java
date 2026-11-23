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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.LineageFileIdentity;
import org.apache.hop.lineage.LineageRelationalIdentity;
import org.apache.hop.lineage.model.RelationalTable;

/**
 * A dataset as Hop knows it, for a data catalog to resolve. Tables and files get the OpenLineage
 * {@code namespace} and {@code name} that lineage gives them, so a catalog finds the same dataset
 * lineage wrote to it. A dataset can also be named by its id in the catalog. Experimental.
 *
 * @param namespace the OpenLineage namespace, for example {@code postgres://db:5432}; null for a
 *     catalog id
 * @param name the OpenLineage name, for example {@code sales.public.orders}; null for a catalog id
 * @param catalogId the id of the dataset in the catalog, for example a DataHub URN, or null
 */
public record DatasetRef(String namespace, String name, String catalogId) {

  /** A dataset by its OpenLineage identity. */
  public static DatasetRef of(String namespace, String name) {
    return new DatasetRef(namespace, name, null);
  }

  /** A dataset by its id in the catalog. */
  public static DatasetRef ofCatalogId(String catalogId) {
    return new DatasetRef(null, null, catalogId);
  }

  /**
   * A table of a Hop relational connection. A blank schema is filled with the connection's
   * preferred schema and the database with the connection's database, the way lineage qualifies the
   * tables it reports.
   *
   * @param databaseMeta the connection
   * @param variables to resolve the connection's host, port, database and preferred schema
   * @param schema the schema, or blank
   * @param table the table
   */
  public static DatasetRef ofTable(
      DatabaseMeta databaseMeta, IVariables variables, String schema, String table) {
    String namespace = LineageRelationalIdentity.namespace(databaseMeta, variables);
    RelationalTable qualified =
        LineageRelationalIdentity.withDefaults(
            new RelationalTable(null, StringUtils.trimToNull(schema), table),
            variables.resolve(databaseMeta.getDatabaseName()),
            variables.resolve(databaseMeta.getPreferredSchemaName()));
    return of(namespace, LineageRelationalIdentity.datasetName(qualified));
  }

  /**
   * A file, by its name in Hop: a path, a URL or a VFS name.
   *
   * @throws HopException when the name can't be turned into a file
   */
  public static DatasetRef ofFile(String filename, IVariables variables) throws HopException {
    FileObject file = HopVfs.getFileObject(variables.resolve(filename), variables);
    LineageFileIdentity.Identity identity = LineageFileIdentity.of(file.getName().getURI());
    return identity == null ? of(null, null) : of(identity.namespace(), identity.name());
  }

  /** Whether this names a dataset a catalog can look for. */
  public boolean isComplete() {
    return StringUtils.isNotBlank(catalogId)
        || (StringUtils.isNotBlank(namespace) && StringUtils.isNotBlank(name));
  }

  /** How to name this dataset in a log line. */
  public String describe() {
    if (StringUtils.isNotBlank(catalogId)) {
      return catalogId;
    }
    return StringUtils.defaultString(namespace) + " " + StringUtils.defaultString(name);
  }
}
