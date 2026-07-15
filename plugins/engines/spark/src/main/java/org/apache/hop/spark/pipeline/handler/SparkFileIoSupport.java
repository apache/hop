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

package org.apache.hop.spark.pipeline.handler;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.spark.transforms.io.SparkFileInputMeta;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

/** Shared helpers for Spark file source/sink handlers. */
public final class SparkFileIoSupport {

  private SparkFileIoSupport() {}

  public static String normalizeFormat(String format) {
    if (StringUtils.isEmpty(format)) {
      return SparkFileInputMeta.FORMAT_CSV;
    }
    return format.trim().toLowerCase();
  }

  public static Map<String, String> parseExtraOptions(IVariables variables, String extraOptions) {
    Map<String, String> options = new LinkedHashMap<>();
    if (StringUtils.isEmpty(extraOptions)) {
      return options;
    }
    for (String line : extraOptions.split("\\r?\\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      int eq = trimmed.indexOf('=');
      if (eq > 0) {
        String key = variables.resolve(trimmed.substring(0, eq).trim());
        String value = variables.resolve(trimmed.substring(eq + 1).trim());
        options.put(key, value);
      }
    }
    return options;
  }

  public static SaveMode toSaveMode(String mode) throws HopException {
    if (StringUtils.isEmpty(mode)) {
      return SaveMode.Overwrite;
    }
    return switch (mode.trim()) {
      case "Overwrite", "overwrite" -> SaveMode.Overwrite;
      case "Append", "append" -> SaveMode.Append;
      case "Ignore", "ignore" -> SaveMode.Ignore;
      case "ErrorIfExists", "error", "Error" -> SaveMode.ErrorIfExists;
      default -> throw new HopException("Unknown Spark save mode: " + mode);
    };
  }

  public static void writeDataset(
      Dataset<Row> dataset,
      String format,
      String path,
      SaveMode saveMode,
      Map<String, String> options,
      String[] partitionColumns,
      Integer coalesce)
      throws HopException {
    Dataset<Row> toWrite = dataset;
    if (coalesce != null && coalesce > 0) {
      toWrite = toWrite.coalesce(coalesce);
    }
    DataFrameWriter<Row> writer = toWrite.write().mode(saveMode).format(format);
    for (Map.Entry<String, String> e : options.entrySet()) {
      writer = writer.option(e.getKey(), e.getValue());
    }
    if (partitionColumns != null && partitionColumns.length > 0) {
      writer = writer.partitionBy(partitionColumns);
    }
    try {
      writer.save(path);
    } catch (Exception e) {
      throw new HopException("Error writing Spark Dataset to '" + path + "' as " + format, e);
    }
  }
}
