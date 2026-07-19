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

package org.apache.hop.spark.table;

/**
 * Format identifiers and well-known class / Spark conf names for open table formats on the native
 * Spark engine. Connector JARs are optional at runtime (see {@link SparkLakeConnectorProbe}).
 */
public final class SparkLakeFormats {

  public static final String FORMAT_DELTA = "delta";
  public static final String FORMAT_ICEBERG = "iceberg";

  /** Delta SparkSession extension (must be set at session build when Delta is used). */
  public static final String DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension";

  /**
   * Delta catalog implementation for {@code spark.sql.catalog.spark_catalog}. Required for modern
   * Delta 4.x on Spark 4 (SQL / MERGE / OPTIMIZE); hop-run always sets it when Delta is needed.
   */
  public static final String DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";

  public static final String SPARK_CONF_EXTENSIONS = "spark.sql.extensions";
  public static final String SPARK_CONF_SPARK_CATALOG = "spark.sql.catalog.spark_catalog";

  /** Iceberg SparkSession extensions. */
  public static final String ICEBERG_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  /** Default Iceberg catalog implementation (Hadoop / REST via catalog conf). */
  public static final String ICEBERG_CATALOG = "org.apache.iceberg.spark.SparkCatalog";

  /**
   * Built-in Hadoop catalog name used for Iceberg PATH mode ({@code hop_iceberg.`file:///…`}).
   * Distinct from {@code spark_catalog} so Delta can keep DeltaCatalog when both formats co-exist.
   */
  public static final String ICEBERG_PATH_CATALOG_NAME = "hop_iceberg";

  public static final String SPARK_CONF_ICEBERG_PATH_CATALOG =
      "spark.sql.catalog." + ICEBERG_PATH_CATALOG_NAME;
  public static final String SPARK_CONF_ICEBERG_PATH_CATALOG_TYPE =
      SPARK_CONF_ICEBERG_PATH_CATALOG + ".type";
  public static final String SPARK_CONF_ICEBERG_PATH_CATALOG_WAREHOUSE =
      SPARK_CONF_ICEBERG_PATH_CATALOG + ".warehouse";

  private SparkLakeFormats() {}
}
