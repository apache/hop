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
 */

package org.apache.hop.lakehouse.iceberg;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lakehouse.iceberg.io.HopVfsFileIO;
import org.apache.hop.lakehouse.metadata.LakeCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/** Finds Iceberg tables for the local engine, either at a path or through a catalog. */
public final class IcebergTables {

  /** Hadoop catalogs and Spark path tables name metadata files v1.metadata.json, v2... */
  private static final Pattern HADOOP_METADATA =
      Pattern.compile("v(\\d+)(\\.gz)?\\.metadata\\.json");

  /** Other catalogs name them 00001-<uuid>.metadata.json, 00002-... */
  private static final Pattern CATALOG_METADATA =
      Pattern.compile("(\\d+)-[0-9a-fA-F-]+(\\.gz)?\\.metadata\\.json");

  private static final String REST_CATALOG = "org.apache.iceberg.rest.RESTCatalog";

  private IcebergTables() {}

  /**
   * Loads the table whose root folder is {@code location}, without a catalog. This is how Hadoop
   * catalogs and Spark path tables store tables: the newest metadata file in {@code metadata/}
   * describes the current state. The table is read-only.
   */
  public static Table loadFromPath(String location) throws HopException {
    String root = StringUtils.removeEnd(location, "/");
    String metadataFile = currentMetadataFile(root);
    FileIO io = new HopVfsFileIO();
    return new BaseTable(new StaticTableOperations(metadataFile, io), root);
  }

  /**
   * Loads a table through the catalog described by a lakehouse catalog metadata object. The
   * identifier is {@code namespace.table}, optionally prefixed with the catalog name (the form the
   * Spark engine uses, for example {@code lake.db.orders}).
   */
  public static Table loadFromCatalog(
      LakeCatalog catalog, String tableIdentifier, IVariables variables) throws HopException {
    IcebergTableTarget target = targetInCatalog(catalog, tableIdentifier, variables);
    if (!target.exists()) {
      throw new HopException("Table " + target.name() + " doesn't exist");
    }
    return target.read();
  }

  /**
   * The table {@code tableIdentifier} in the catalog described by a lakehouse catalog metadata
   * object, which may or may not exist yet.
   */
  public static IcebergTableTarget targetInCatalog(
      LakeCatalog catalog, String tableIdentifier, IVariables variables) throws HopException {
    if (catalog == null) {
      throw new HopException("No catalog specified to look up table '" + tableIdentifier + "'");
    }
    String type = StringUtils.defaultIfBlank(catalog.getCatalogType(), LakeCatalog.TYPE_HADOOP);
    String catalogName =
        StringUtils.defaultIfBlank(variables.resolve(catalog.getCatalogName()), catalog.getName());
    TableIdentifier identifier = toIdentifier(variables.resolve(tableIdentifier), catalogName);

    switch (type) {
      case LakeCatalog.TYPE_HADOOP -> {
        String warehouse = variables.resolve(catalog.getWarehouse());
        if (StringUtils.isBlank(warehouse)) {
          throw new HopException(
              "Catalog '" + catalog.getName() + "' (hadoop) needs a warehouse location");
        }
        return IcebergTableTarget.atPath(hadoopTableLocation(warehouse, identifier));
      }
      case LakeCatalog.TYPE_REST -> {
        Catalog rest =
            CatalogUtil.loadCatalog(
                REST_CATALOG, catalogName, restProperties(catalog, variables), null);
        return IcebergTableTarget.inCatalog(rest, identifier);
      }
      default ->
          throw new HopException(
              "Catalog type '"
                  + type
                  + "' of catalog '"
                  + catalog.getName()
                  + "' is only supported on the Spark engine for now. The local engine supports"
                  + " the hadoop and rest catalog types.");
    }
  }

  static TableIdentifier toIdentifier(String tableIdentifier, String catalogName)
      throws HopException {
    if (StringUtils.isBlank(tableIdentifier)) {
      throw new HopException("No table identifier specified");
    }
    String[] parts = tableIdentifier.split("\\.");
    if (parts.length > 2 && parts[0].equals(catalogName)) {
      parts = Arrays.copyOfRange(parts, 1, parts.length);
    }
    if (parts.length < 2) {
      throw new HopException(
          "Table identifier '" + tableIdentifier + "' needs a namespace, for example db.orders");
    }
    return TableIdentifier.of(
        Namespace.of(Arrays.copyOf(parts, parts.length - 1)), parts[parts.length - 1]);
  }

  /** Where a Hadoop catalog keeps a table: warehouse/ns1/ns2/table. */
  static String hadoopTableLocation(String warehouse, TableIdentifier identifier) {
    StringBuilder location = new StringBuilder(StringUtils.removeEnd(warehouse, "/"));
    for (String level : identifier.namespace().levels()) {
      location.append('/').append(level);
    }
    return location.append('/').append(identifier.name()).toString();
  }

  /**
   * REST catalog properties. Files go through Hop VFS unless the extra configuration names another
   * FileIO, for example S3FileIO with credentials vended by the catalog.
   */
  static Map<String, String> restProperties(LakeCatalog catalog, IVariables variables)
      throws HopException {
    String uri = variables.resolve(catalog.getUri());
    if (StringUtils.isBlank(uri)) {
      throw new HopException("Catalog '" + catalog.getName() + "' (rest) needs a URI");
    }
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, uri);
    properties.put(CatalogProperties.FILE_IO_IMPL, HopVfsFileIO.class.getName());
    String warehouse = variables.resolve(catalog.getWarehouse());
    if (StringUtils.isNotBlank(warehouse)) {
      properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    }
    String credential = variables.resolve(catalog.getCredential());
    if (StringUtils.isNotBlank(credential)) {
      properties.put("token", credential);
    }
    properties.putAll(extraProperties(variables.resolve(catalog.getConfExtra())));
    return properties;
  }

  /**
   * Extra catalog properties, one key=value per line. Spark-style keys ({@code
   * spark.sql.catalog.<name>.<key>}) are accepted too, so one catalog definition works on both
   * engines.
   */
  static Map<String, String> extraProperties(String confExtra) {
    Map<String, String> properties = new HashMap<>();
    if (StringUtils.isBlank(confExtra)) {
      return properties;
    }
    for (String line : confExtra.split("\\R")) {
      String trimmed = line.trim();
      int equals = trimmed.indexOf('=');
      if (trimmed.isEmpty() || trimmed.startsWith("#") || equals <= 0) {
        continue;
      }
      String key = trimmed.substring(0, equals).trim();
      if (key.startsWith("spark.sql.catalog.")) {
        int dot = key.indexOf('.', "spark.sql.catalog.".length());
        if (dot < 0) {
          continue;
        }
        key = key.substring(dot + 1);
      }
      properties.put(key, trimmed.substring(equals + 1).trim());
    }
    return properties;
  }

  /** The current metadata file of the table at {@code location}. */
  static String currentMetadataFile(String location) throws HopException {
    String metadataFile = findCurrentMetadataFile(location);
    if (metadataFile == null) {
      throw new HopException(
          "No Iceberg table found at '" + location + "': there is no table metadata file");
    }
    return metadataFile;
  }

  /**
   * The current metadata file of the table at {@code location}, or null if there is no table there.
   * The version hint is followed when there is one; otherwise the newest metadata file is used.
   */
  static String findCurrentMetadataFile(String location) throws HopException {
    String root = StringUtils.removeEnd(location, "/");
    String metadataFolder = root + "/metadata";
    try {
      FileObject hint = HopVfs.getFileObject(metadataFolder + "/version-hint.text");
      if (hint.exists()) {
        try (InputStream in = HopVfs.getInputStream(hint)) {
          String version = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
          String file = metadataFolder + "/v" + version + ".metadata.json";
          if (HopVfs.getFileObject(file).exists()) {
            return file;
          }
        }
      }

      FileObject folder = HopVfs.getFileObject(metadataFolder);
      if (!folder.exists()) {
        return null;
      }
      String newest = null;
      long newestVersion = -1;
      for (FileObject child : folder.getChildren()) {
        String name = child.getName().getBaseName();
        long version = metadataVersion(name);
        if (version > newestVersion) {
          newestVersion = version;
          newest = name;
        }
      }
      return newest == null ? null : metadataFolder + "/" + newest;
    } catch (Exception e) {
      throw new HopException("Unable to find the Iceberg metadata of table '" + root + "'", e);
    }
  }

  /** Version number in a metadata file name, or -1 if it isn't a table metadata file. */
  static long metadataVersion(String fileName) {
    for (Pattern pattern : new Pattern[] {HADOOP_METADATA, CATALOG_METADATA}) {
      Matcher matcher = pattern.matcher(fileName);
      if (matcher.matches()) {
        return Long.parseLong(matcher.group(1));
      }
    }
    return -1;
  }
}
