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

package org.apache.hop.parquet.explorer;

import java.util.List;
import lombok.Getter;
import org.apache.hop.core.row.IRowMeta;

/**
 * What the explorer shows for one Parquet file. Sizes that the file does not record are null.
 * {@link #previewError} is set when the footer could be read but the rows could not.
 */
@Getter
public class ParquetFileInspection {
  private final String fileName;
  private final String folder;
  private final long sizeBytes;
  private final String compression;
  private final String version;
  private final Long rowGroupSize;
  private final Long dataPageSize;
  private final Long dictionaryPageSize;
  private final long rowCount;
  private final int rowGroupCount;
  private final String createdBy;
  private final String schemaJson;
  private final List<ParquetColumnView> columns;
  private final IRowMeta rowMeta;
  private final List<Object[]> previewRows;
  private final String previewError;

  public ParquetFileInspection(
      String fileName,
      String folder,
      long sizeBytes,
      String compression,
      String version,
      Long rowGroupSize,
      Long dataPageSize,
      Long dictionaryPageSize,
      long rowCount,
      int rowGroupCount,
      String createdBy,
      String schemaJson,
      List<ParquetColumnView> columns,
      IRowMeta rowMeta,
      List<Object[]> previewRows,
      String previewError) {
    this.fileName = fileName;
    this.folder = folder;
    this.sizeBytes = sizeBytes;
    this.compression = compression;
    this.version = version;
    this.rowGroupSize = rowGroupSize;
    this.dataPageSize = dataPageSize;
    this.dictionaryPageSize = dictionaryPageSize;
    this.rowCount = rowCount;
    this.rowGroupCount = rowGroupCount;
    this.createdBy = createdBy;
    this.schemaJson = schemaJson;
    this.columns = List.copyOf(columns);
    this.rowMeta = rowMeta;
    this.previewRows = List.copyOf(previewRows);
    this.previewError = previewError;
  }
}
