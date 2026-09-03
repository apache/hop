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
package org.apache.hop.parquet.transforms.output;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;

/**
 * What to do with data that is already present in a partition folder. Only applies when the
 * transform is partitioning its output; without partition fields the transform writes a single file
 * as before.
 */
public enum ParquetWriteMode implements IEnumHasCode {
  /** Leave existing files in place and add new ones. This is the pre-partitioning behaviour. */
  Append("append", "ParquetOutput.WriteMode.Append"),

  /** Empty a partition folder before the first file of this run is written into it. */
  OverwritePartitions("overwrite_partitions", "ParquetOutput.WriteMode.OverwritePartitions"),

  /** Fail if a partition folder already exists. */
  FailIfExists("fail_if_exists", "ParquetOutput.WriteMode.FailIfExists"),

  /** Empty the whole base folder once, before the first file of this run is written. */
  OverwriteAll("overwrite_all", "ParquetOutput.WriteMode.OverwriteAll");

  private static final Class<?> PKG = ParquetOutputMeta.class;

  private final String code;
  private final String descriptionKey;

  ParquetWriteMode(String code, String descriptionKey) {
    this.code = code;
    this.descriptionKey = descriptionKey;
  }

  public static String[] getDescriptions() {
    String[] descriptions = new String[values().length];
    for (int i = 0; i < descriptions.length; i++) {
      descriptions[i] = values()[i].getDescription();
    }
    return descriptions;
  }

  public static ParquetWriteMode getModeFromDescription(String description) {
    for (ParquetWriteMode mode : values()) {
      if (mode.getDescription().equals(description)) {
        return mode;
      }
    }
    return Append;
  }

  @Override
  public String getCode() {
    return code;
  }

  public String getDescription() {
    return BaseMessages.getString(PKG, descriptionKey);
  }
}
