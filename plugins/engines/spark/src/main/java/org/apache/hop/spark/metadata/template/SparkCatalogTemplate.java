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

package org.apache.hop.spark.metadata.template;

import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.spark.metadata.SparkCatalog;
import org.apache.hop.spark.table.SparkLakeFormats;

/**
 * Named presets for {@link SparkCatalog} fields. Pure data — no SWT. Apply via {@link
 * #applyTo(SparkCatalog)}.
 *
 * <p>Advanced presets include a commented {@code # docs: …} line in conf extra (ignored by {@code
 * SparkCatalogApplier}) so operators can open vendor documentation without leaving Hop.
 */
public enum SparkCatalogTemplate {
  ICEBERG_HADOOP_LOCAL(
      "Iceberg Hadoop (local)",
      "Named Iceberg Hadoop catalog on a local warehouse path. Use TABLE mode as lake.db.table.",
      "lake",
      SparkCatalog.TYPE_HADOOP,
      SparkLakeFormats.ICEBERG_CATALOG,
      "file:///tmp/hop-warehouse",
      "",
      confWithDocs(Docs.ICEBERG_SPARK, "")),
  ICEBERG_HADOOP_OBJECT_STORE(
      "Iceberg Hadoop (object store)",
      "Iceberg Hadoop warehouse on object storage (replace bucket). Credentials via env/Hadoop conf.",
      "lake",
      SparkCatalog.TYPE_HADOOP,
      SparkLakeFormats.ICEBERG_CATALOG,
      "s3a://bucket/warehouse",
      "",
      confWithDocs(Docs.ICEBERG_AWS, "io-impl=org.apache.iceberg.aws.s3.S3FileIO")),
  ICEBERG_REST(
      "Iceberg REST",
      "Iceberg REST catalog. Replace the URI with your catalog service endpoint.",
      "lake",
      SparkCatalog.TYPE_REST,
      SparkLakeFormats.ICEBERG_CATALOG,
      "",
      "https://catalog.example.com/v1",
      confWithDocs(Docs.ICEBERG_REST, "")),
  ICEBERG_REST_AUTH(
      "Iceberg REST (authenticated)",
      "Iceberg REST with token auth. Put the secret in Credential (maps to catalog .token).",
      "lake",
      SparkCatalog.TYPE_REST,
      SparkLakeFormats.ICEBERG_CATALOG,
      "",
      "https://catalog.example.com/v1",
      confWithDocs(
          Docs.ICEBERG_REST,
          "# Bearer/token: use the Credential field (applied as spark.sql.catalog.<name>.token)")),
  HIVE_METASTORE(
      "Hive Metastore (advanced)",
      "Iceberg via Hive Metastore. Requires Hive/Iceberg deps on the cluster (not packaged by default).",
      "hive",
      SparkCatalog.TYPE_HIVE,
      SparkLakeFormats.ICEBERG_CATALOG,
      "",
      "",
      confWithDocs(Docs.ICEBERG_HIVE, "uri=thrift://hive-metastore:9083")),
  AWS_GLUE(
      "AWS Glue (advanced)",
      "Iceberg via AWS Glue. Requires AWS/Iceberg deps and IAM on the cluster (not packaged by default).",
      "glue",
      SparkCatalog.TYPE_GLUE,
      SparkLakeFormats.ICEBERG_CATALOG,
      "s3a://bucket/warehouse",
      "",
      confWithDocs(
          Docs.ICEBERG_AWS,
          "warehouse=s3a://bucket/warehouse\nio-impl=org.apache.iceberg.aws.s3.S3FileIO")),
  NESSIE(
      "Nessie (advanced)",
      "Iceberg Nessie catalog. Requires Nessie/Iceberg deps; replace URI, ref, and warehouse.",
      "nessie",
      SparkCatalog.TYPE_CUSTOM,
      SparkLakeFormats.ICEBERG_CATALOG,
      "file:///tmp/nessie-warehouse",
      "http://localhost:19120/api/v1",
      confWithDocs(
          Docs.NESSIE_SPARK,
          "catalog-impl=org.apache.iceberg.nessie.NessieCatalog\n"
              + "uri=http://localhost:19120/api/v1\n"
              + "ref=main\n"
              + "warehouse=file:///tmp/nessie-warehouse")),
  DATABRICKS_UNITY(
      "Databricks Unity Catalog (advanced)",
      "Skeleton for Unity Catalog on Databricks. Prefer workspace-managed conf; adjust URI/auth for your env.",
      "unity",
      SparkCatalog.TYPE_CUSTOM,
      SparkLakeFormats.ICEBERG_CATALOG,
      "",
      "",
      confWithDocs(
          Docs.DATABRICKS_UNITY,
          "# Typically configured on the Databricks cluster or spark-submit.\n"
              + "# Example short keys (uncomment and replace if your runtime expects them):\n"
              + "# uri=https://<workspace>/api/2.1/unity-catalog\n"
              + "# warehouse=s3://<bucket>/unity")),
  DELTA_NAMED_CATALOG(
      "Delta named catalog (advanced)",
      "Named DeltaCatalog (not spark_catalog). Prefer spark_catalog for Delta when coexisting with Iceberg.",
      "delta_cat",
      SparkCatalog.TYPE_CUSTOM,
      SparkLakeFormats.DELTA_CATALOG,
      "",
      "",
      confWithDocs(
          Docs.DELTA,
          "# Prefer spark.sql.catalog.spark_catalog=DeltaCatalog for default Delta tables.\n"
              + "# Use a distinct catalog name only when you must register a second Delta catalog."));

  /**
   * Documentation URLs for conf-extra {@code # docs:} lines. Nested interface so enum constants can
   * reference them without illegal forward references.
   */
  public interface Docs {
    String ICEBERG_SPARK = "https://iceberg.apache.org/docs/latest/spark-configuration/";
    String ICEBERG_REST = "https://iceberg.apache.org/docs/latest/rest-catalog/";
    String ICEBERG_AWS = "https://iceberg.apache.org/docs/latest/aws/";
    String ICEBERG_HIVE = "https://iceberg.apache.org/docs/latest/hive/";
    String NESSIE_SPARK = "https://projectnessie.org/iceberg/spark/";
    String DATABRICKS_UNITY =
        "https://docs.databricks.com/en/data-governance/unity-catalog/index.html";
    String DELTA = "https://docs.delta.io/latest/index.html";
  }

  private final String displayName;
  private final String description;
  private final String catalogName;
  private final String catalogType;
  private final String implementation;
  private final String warehouse;
  private final String uri;
  private final String confExtra;

  SparkCatalogTemplate(
      String displayName,
      String description,
      String catalogName,
      String catalogType,
      String implementation,
      String warehouse,
      String uri,
      String confExtra) {
    this.displayName = displayName;
    this.description = description;
    this.catalogName = catalogName;
    this.catalogType = catalogType;
    this.implementation = implementation;
    this.warehouse = warehouse;
    this.uri = uri;
    this.confExtra = confExtra;
  }

  /**
   * Build conf-extra text with a leading commented documentation URL (ignored by the applier) and
   * optional body lines.
   */
  static String confWithDocs(String docsUrl, String body) {
    String header = "# docs: " + docsUrl;
    if (StringUtils.isBlank(body)) {
      return header;
    }
    return header + "\n" + body;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getDescription() {
    return description;
  }

  /** Labels for selection dialogs (same order as {@link #values()}). */
  public static String[] displayNames() {
    return Arrays.stream(values()).map(SparkCatalogTemplate::getDisplayName).toArray(String[]::new);
  }

  public static SparkCatalogTemplate fromDisplayName(String name) {
    if (name == null) {
      return null;
    }
    for (SparkCatalogTemplate t : values()) {
      if (t.displayName.equals(name)) {
        return t;
      }
    }
    return null;
  }

  /**
   * Apply this template onto {@code catalog}. Does not change the Hop metadata object name; leaves
   * credential empty (never put secrets in presets).
   */
  public void applyTo(SparkCatalog catalog) {
    Objects.requireNonNull(catalog, "catalog");
    catalog.setCatalogName(catalogName);
    catalog.setCatalogType(catalogType);
    catalog.setImplementation(implementation);
    catalog.setWarehouse(warehouse);
    catalog.setUri(uri);
    catalog.setCredential("");
    catalog.setConfExtra(confExtra);
  }

  /**
   * True if the catalog looks customized relative to a fresh {@link SparkCatalog} default (used for
   * overwrite confirmation).
   */
  public static boolean looksCustomized(SparkCatalog catalog) {
    if (catalog == null) {
      return false;
    }
    SparkCatalog def = new SparkCatalog();
    if (StringUtils.isNotEmpty(catalog.getCatalogName())) {
      return true;
    }
    if (!Objects.equals(nz(catalog.getCatalogType()), nz(def.getCatalogType()))) {
      return true;
    }
    if (!Objects.equals(nz(catalog.getImplementation()), nz(def.getImplementation()))) {
      return true;
    }
    if (StringUtils.isNotEmpty(catalog.getWarehouse())
        || StringUtils.isNotEmpty(catalog.getUri())
        || StringUtils.isNotEmpty(catalog.getCredential())
        || StringUtils.isNotEmpty(catalog.getConfExtra())) {
      return true;
    }
    return false;
  }

  private static String nz(String s) {
    return s == null ? "" : s;
  }
}
