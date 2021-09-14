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

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "ParquetFileOutput",
    image = "parquet_output.svg",
    name = "i18n::ParquetOutput.Name",
    description = "i18n::ParquetOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/parquet-file-output.html",
    keywords = {"parquet", "write", "file", "column"})
public class ParquetOutputMeta extends BaseTransformMeta
    implements ITransformMeta<ParquetOutput, ParquetOutputData> {

  @HopMetadataProperty(key = "filename_base")
  private String filenameBase;

  @HopMetadataProperty(key = "filename_ext")
  private String filenameExtension;

  @HopMetadataProperty(key = "filename_include_date")
  private boolean filenameIncludingDate;

  @HopMetadataProperty(key = "filename_include_time")
  private boolean filenameIncludingTime;

  @HopMetadataProperty(key = "filename_include_datetime")
  private boolean filenameIncludingDateTime;

  @HopMetadataProperty(key = "filename_datetime_format")
  private String filenameDateTimeFormat;

  @HopMetadataProperty(key = "filename_include_copy")
  private boolean filenameIncludingCopyNr;

  @HopMetadataProperty(key = "filename_include_split")
  private boolean filenameIncludingSplitNr;

  @HopMetadataProperty(key = "filename_split_size")
  private String fileSplitSize;

  @HopMetadataProperty(key = "filename_create_parent_folders")
  private boolean filenameCreatingParentFolders;

  @HopMetadataProperty(key = "compression_codec")
  private CompressionCodecName compressionCodec;

  @HopMetadataProperty(key = "version", storeWithCode = true)
  private ParquetVersion version;

  @HopMetadataProperty(key = "row_group_size")
  private String rowGroupSize;

  @HopMetadataProperty(key = "data_page_size")
  private String dataPageSize;

  @HopMetadataProperty(key = "dictionary_page_size")
  private String dictionaryPageSize;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<ParquetField> fields;

  public ParquetOutputMeta() {
    filenameExtension = "parquet";
    filenameDateTimeFormat = "yyyyMMdd-HHmmss";
    compressionCodec = CompressionCodecName.UNCOMPRESSED;
    version = ParquetVersion.Version1; // The default is v1
    rowGroupSize = Integer.toString(ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
    dataPageSize = Integer.toString(ParquetProperties.DEFAULT_PAGE_SIZE);
    dictionaryPageSize = Integer.toString(ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
    fields = new ArrayList<>();
    filenameIncludingCopyNr = true;
    filenameIncludingSplitNr = true;
    filenameCreatingParentFolders = true;
    fileSplitSize = "1000000";
  }

  public ParquetOutputMeta(ParquetOutputMeta m) {
    this.filenameBase = m.filenameBase;
    this.filenameExtension = m.filenameExtension;
    this.filenameIncludingDate = m.filenameIncludingDate;
    this.filenameIncludingTime = m.filenameIncludingTime;
    this.filenameIncludingDateTime = m.filenameIncludingDateTime;
    this.filenameDateTimeFormat = m.filenameDateTimeFormat;
    this.filenameIncludingCopyNr = m.filenameIncludingCopyNr;
    this.filenameIncludingSplitNr = m.filenameIncludingSplitNr;
    this.fileSplitSize = m.fileSplitSize;
    this.filenameCreatingParentFolders = m.filenameCreatingParentFolders;
    this.compressionCodec = m.compressionCodec;
    this.version = m.version;
    this.rowGroupSize = m.rowGroupSize;
    this.dataPageSize = m.dataPageSize;
    this.dictionaryPageSize = m.dictionaryPageSize;
    this.fields = m.fields;
  }

  @Override
  public ParquetOutputData getTransformData() {
    return new ParquetOutputData();
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ParquetOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ParquetOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Gets filenameBase
   *
   * @return value of filenameBase
   */
  public String getFilenameBase() {
    return filenameBase;
  }

  /** @param filenameBase The filenameBase to set */
  public void setFilenameBase(String filenameBase) {
    this.filenameBase = filenameBase;
  }

  /**
   * Gets filenameExtension
   *
   * @return value of filenameExtension
   */
  public String getFilenameExtension() {
    return filenameExtension;
  }

  /** @param filenameExtension The filenameExtension to set */
  public void setFilenameExtension(String filenameExtension) {
    this.filenameExtension = filenameExtension;
  }

  /**
   * Gets filenameIncludingDate
   *
   * @return value of filenameIncludingDate
   */
  public boolean isFilenameIncludingDate() {
    return filenameIncludingDate;
  }

  /** @param filenameIncludingDate The filenameIncludingDate to set */
  public void setFilenameIncludingDate(boolean filenameIncludingDate) {
    this.filenameIncludingDate = filenameIncludingDate;
  }

  /**
   * Gets filenameIncludingTime
   *
   * @return value of filenameIncludingTime
   */
  public boolean isFilenameIncludingTime() {
    return filenameIncludingTime;
  }

  /** @param filenameIncludingTime The filenameIncludingTime to set */
  public void setFilenameIncludingTime(boolean filenameIncludingTime) {
    this.filenameIncludingTime = filenameIncludingTime;
  }

  /**
   * Gets filenameIncludingDateTime
   *
   * @return value of filenameIncludingDateTime
   */
  public boolean isFilenameIncludingDateTime() {
    return filenameIncludingDateTime;
  }

  /** @param filenameIncludingDateTime The filenameIncludingDateTime to set */
  public void setFilenameIncludingDateTime(boolean filenameIncludingDateTime) {
    this.filenameIncludingDateTime = filenameIncludingDateTime;
  }

  /**
   * Gets filenameDateTimeFormat
   *
   * @return value of filenameDateTimeFormat
   */
  public String getFilenameDateTimeFormat() {
    return filenameDateTimeFormat;
  }

  /** @param filenameDateTimeFormat The filenameDateTimeFormat to set */
  public void setFilenameDateTimeFormat(String filenameDateTimeFormat) {
    this.filenameDateTimeFormat = filenameDateTimeFormat;
  }

  /**
   * Gets filenameIncludingCopyNr
   *
   * @return value of filenameIncludingCopyNr
   */
  public boolean isFilenameIncludingCopyNr() {
    return filenameIncludingCopyNr;
  }

  /** @param filenameIncludingCopyNr The filenameIncludingCopyNr to set */
  public void setFilenameIncludingCopyNr(boolean filenameIncludingCopyNr) {
    this.filenameIncludingCopyNr = filenameIncludingCopyNr;
  }

  /**
   * Gets filenameIncludingSplitNr
   *
   * @return value of filenameIncludingSplitNr
   */
  public boolean isFilenameIncludingSplitNr() {
    return filenameIncludingSplitNr;
  }

  /** @param filenameIncludingSplitNr The filenameIncludingSplitNr to set */
  public void setFilenameIncludingSplitNr(boolean filenameIncludingSplitNr) {
    this.filenameIncludingSplitNr = filenameIncludingSplitNr;
  }

  /**
   * Gets filenameIncludingSplitSize
   *
   * @return value of filenameIncludingSplitSize
   */
  public String getFileSplitSize() {
    return fileSplitSize;
  }

  /** @param fileSplitSize The filenameIncludingSplitSize to set */
  public void setFileSplitSize(String fileSplitSize) {
    this.fileSplitSize = fileSplitSize;
  }

  /**
   * Gets filenameCreatingParentFolders
   *
   * @return value of filenameCreatingParentFolders
   */
  public boolean isFilenameCreatingParentFolders() {
    return filenameCreatingParentFolders;
  }

  /** @param filenameCreatingParentFolders The filenameCreatingParentFolders to set */
  public void setFilenameCreatingParentFolders(boolean filenameCreatingParentFolders) {
    this.filenameCreatingParentFolders = filenameCreatingParentFolders;
  }

  /**
   * Gets compressionCodec
   *
   * @return value of compressionCodec
   */
  public CompressionCodecName getCompressionCodec() {
    return compressionCodec;
  }

  /** @param compressionCodec The compressionCodec to set */
  public void setCompressionCodec(CompressionCodecName compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  /**
   * Gets version
   *
   * @return value of version
   */
  public ParquetVersion getVersion() {
    return version;
  }

  /** @param version The version to set */
  public void setVersion(ParquetVersion version) {
    this.version = version;
  }

  /**
   * Gets rowGroupSize
   *
   * @return value of rowGroupSize
   */
  public String getRowGroupSize() {
    return rowGroupSize;
  }

  /** @param rowGroupSize The rowGroupSize to set */
  public void setRowGroupSize(String rowGroupSize) {
    this.rowGroupSize = rowGroupSize;
  }

  /**
   * Gets dataPageSize
   *
   * @return value of dataPageSize
   */
  public String getDataPageSize() {
    return dataPageSize;
  }

  /** @param dataPageSize The dataPageSize to set */
  public void setDataPageSize(String dataPageSize) {
    this.dataPageSize = dataPageSize;
  }

  /**
   * Gets dictionaryPageSize
   *
   * @return value of dictionaryPageSize
   */
  public String getDictionaryPageSize() {
    return dictionaryPageSize;
  }

  /** @param dictionaryPageSize The dictionaryPageSize to set */
  public void setDictionaryPageSize(String dictionaryPageSize) {
    this.dictionaryPageSize = dictionaryPageSize;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<ParquetField> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<ParquetField> fields) {
    this.fields = fields;
  }
}
