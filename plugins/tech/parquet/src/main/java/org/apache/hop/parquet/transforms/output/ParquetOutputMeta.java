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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

@Getter
@Setter
@Transform(
    id = "ParquetFileOutput",
    image = "parquet_output.svg",
    name = "i18n::ParquetOutput.Name",
    description = "i18n::ParquetOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/parquet-file-output.html",
    keywords = "i18n::ParquetOutputMeta.keyword")
@GuiPlugin
public class ParquetOutputMeta extends BaseTransformMeta<ParquetOutput, ParquetOutputData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "PARQUET_OUTPUT_DIALOG_OPTIONS";

  public static final String GROUP_FILE = "i18n::ParquetOutputMeta.Group.File";
  public static final String GROUP_OPTIONS = "i18n::ParquetOutputMeta.Group.Options";
  public static final String GROUP_PARTITIONING = "i18n::ParquetOutputMeta.Group.Partitioning";
  public static final String GROUP_FIELDS = "i18n::ParquetOutputMeta.Group.Fields";

  public static final String WIDGET_FILENAME_BASE = "filenameBase";
  public static final String WIDGET_FILENAME_EXTENSION = "filenameExtension";
  public static final String WIDGET_FILENAME_INCLUDE_DATE = "filenameIncludingDate";
  public static final String WIDGET_FILENAME_INCLUDE_TIME = "filenameIncludingTime";
  public static final String WIDGET_FILENAME_INCLUDE_DATETIME = "filenameIncludingDateTime";
  public static final String WIDGET_FILENAME_DATETIME_FORMAT = "filenameDateTimeFormat";
  public static final String WIDGET_FILENAME_INCLUDE_COPY_NR = "filenameIncludingCopyNr";
  public static final String WIDGET_FILENAME_INCLUDE_SPLIT_NR = "filenameIncludingSplitNr";
  public static final String WIDGET_FILE_SPLIT_SIZE = "fileSplitSize";
  public static final String WIDGET_FILENAME_CREATE_FOLDERS = "filenameCreatingParentFolders";
  public static final String WIDGET_FILENAME_COMPRESSION_BEFORE_EXTENSION =
      "filenameCompressionBeforeExtension";
  public static final String WIDGET_COMPRESSION_CODEC = "compressionCodec";
  public static final String WIDGET_VERSION = "version";
  public static final String WIDGET_ROW_GROUP_SIZE = "rowGroupSize";
  public static final String WIDGET_DATA_PAGE_SIZE = "dataPageSize";
  public static final String WIDGET_DICTIONARY_PAGE_SIZE = "dictionaryPageSize";
  public static final String WIDGET_WRITE_MODE = "writeMode";
  public static final String WIDGET_MAX_OPEN_PARTITIONS = "maxOpenPartitions";

  @GuiWidgetElement(
      id = WIDGET_FILENAME_BASE,
      order = "0100",
      type = GuiElementType.FILENAME,
      typeFilename = ParquetTypeFilename.class,
      namingSchemeType = "file",
      label = "i18n::ParquetOutputDialog.FilenameBase.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameBase.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_base")
  private String filenameBase;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_EXTENSION,
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.FilenameExtension.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameExtension.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_ext")
  private String filenameExtension;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_INCLUDE_DATE,
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameIncludeDate.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameIncludeDate.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_include_date")
  private boolean filenameIncludingDate;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_INCLUDE_TIME,
      order = "0400",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameIncludeTime.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameIncludeTime.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_include_time")
  private boolean filenameIncludingTime;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_INCLUDE_DATETIME,
      order = "0500",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameIncludeDateTime.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameIncludeDateTime.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_include_datetime")
  private boolean filenameIncludingDateTime;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_DATETIME_FORMAT,
      order = "0600",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.FilenameDateTimeFormat.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameDateTimeFormat.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_datetime_format")
  private String filenameDateTimeFormat;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_INCLUDE_COPY_NR,
      order = "0700",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameIncludeCopyNr.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameIncludeCopyNr.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_include_copy")
  private boolean filenameIncludingCopyNr;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_INCLUDE_SPLIT_NR,
      order = "0800",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameIncludeSplitNr.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameIncludeSplitNr.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_include_split")
  private boolean filenameIncludingSplitNr;

  @GuiWidgetElement(
      id = WIDGET_FILE_SPLIT_SIZE,
      order = "0900",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.FilenameSplitSize.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameSplitSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_split_size")
  private String fileSplitSize;

  @GuiWidgetElement(
      id = WIDGET_FILENAME_CREATE_FOLDERS,
      order = "1000",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameCreateFolders.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameCreateFolders.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_create_parent_folders")
  private boolean filenameCreatingParentFolders;

  /**
   * When true, the compression codec extension is placed before the file extension (e.g.
   * file.snappy.parquet). When false, it is appended after the file extension (e.g.
   * file.parquet.snappy). New transforms default to true; pipelines loaded without this property
   * keep false for backward compatibility.
   */
  @GuiWidgetElement(
      id = WIDGET_FILENAME_COMPRESSION_BEFORE_EXTENSION,
      order = "1100",
      type = GuiElementType.CHECKBOX,
      label = "i18n::ParquetOutputDialog.FilenameCompressionBeforeExtension.Label",
      toolTip = "i18n::ParquetOutputMeta.FilenameCompressionBeforeExtension.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_FILE,
      groupOrder = "0100")
  @HopMetadataProperty(key = "filename_compression_before_extension")
  private boolean filenameCompressionBeforeExtension;

  @GuiWidgetElement(
      id = WIDGET_COMPRESSION_CODEC,
      order = "0100",
      type = GuiElementType.COMBO,
      variables = false,
      label = "i18n::ParquetOutputDialog.CompressionCodec.Label",
      toolTip = "i18n::ParquetOutputMeta.CompressionCodec.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0200")
  @HopMetadataProperty(key = "compression_codec")
  private CompressionCodecName compressionCodec;

  @GuiWidgetElement(
      id = WIDGET_VERSION,
      order = "0200",
      type = GuiElementType.COMBO,
      variables = false,
      getterMethod = "getVersionDescription",
      label = "i18n::ParquetOutputDialog.Version.Label",
      toolTip = "i18n::ParquetOutputMeta.Version.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0200")
  @HopMetadataProperty(key = "version", storeWithCode = true)
  private ParquetVersion version;

  @GuiWidgetElement(
      id = WIDGET_ROW_GROUP_SIZE,
      order = "0300",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.RowGroupSize.Label",
      toolTip = "i18n::ParquetOutputMeta.RowGroupSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0200")
  @HopMetadataProperty(key = "row_group_size")
  private String rowGroupSize;

  @GuiWidgetElement(
      id = WIDGET_DATA_PAGE_SIZE,
      order = "0400",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.DataPageSize.Label",
      toolTip = "i18n::ParquetOutputMeta.DataPageSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0200")
  @HopMetadataProperty(key = "data_page_size")
  private String dataPageSize;

  @GuiWidgetElement(
      id = WIDGET_DICTIONARY_PAGE_SIZE,
      order = "0500",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.DictionaryPageSize.Label",
      toolTip = "i18n::ParquetOutputMeta.DictionaryPageSize.Tooltip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_OPTIONS,
      groupOrder = "0200")
  @HopMetadataProperty(key = "dictionary_page_size")
  private String dictionaryPageSize;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<ParquetField> fields;

  @HopMetadataProperty(groupKey = "partition_fields", key = "partition_field")
  private List<ParquetPartitionField> partitionFields;

  @GuiWidgetElement(
      id = WIDGET_WRITE_MODE,
      order = "0100",
      type = GuiElementType.COMBO,
      variables = false,
      getterMethod = "getWriteModeDescription",
      label = "i18n::ParquetOutputDialog.WriteMode.Label",
      toolTip = "i18n::ParquetOutputDialog.WriteMode.ToolTip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_PARTITIONING,
      groupOrder = "0300")
  @HopMetadataProperty(key = "write_mode", storeWithCode = true)
  private ParquetWriteMode writeMode;

  @GuiWidgetElement(
      id = WIDGET_MAX_OPEN_PARTITIONS,
      order = "0200",
      type = GuiElementType.TEXT,
      label = "i18n::ParquetOutputDialog.MaxOpenPartitions.Label",
      toolTip = "i18n::ParquetOutputDialog.MaxOpenPartitions.ToolTip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.TABS,
      group = GROUP_PARTITIONING,
      groupOrder = "0300")
  @HopMetadataProperty(key = "max_open_partitions")
  private String maxOpenPartitions;

  public ParquetOutputMeta() {
    filenameDateTimeFormat = "yyyyMMdd-HHmmss";
    compressionCodec = CompressionCodecName.UNCOMPRESSED;
    version = ParquetVersion.Version2; // The default is v2
    rowGroupSize = Integer.toString(268435456);
    dataPageSize = Integer.toString(8192);
    dictionaryPageSize = Integer.toString(ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
    fields = new ArrayList<>();
    partitionFields = new ArrayList<>();
    writeMode = ParquetWriteMode.Append;
    maxOpenPartitions = "10";
    filenameIncludingCopyNr = true;
    filenameIncludingSplitNr = true;
    filenameCreatingParentFolders = true;
    filenameCompressionBeforeExtension = true;
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
    this.filenameCompressionBeforeExtension = m.filenameCompressionBeforeExtension;
    this.compressionCodec = m.compressionCodec;
    this.version = m.version;
    this.rowGroupSize = m.rowGroupSize;
    this.dataPageSize = m.dataPageSize;
    this.dictionaryPageSize = m.dictionaryPageSize;
    this.fields = new ArrayList<>();
    if (m.fields != null) {
      for (ParquetField field : m.fields) {
        this.fields.add(new ParquetField(field));
      }
    }
    this.partitionFields = new ArrayList<>();
    if (m.partitionFields != null) {
      for (ParquetPartitionField f : m.partitionFields) {
        this.partitionFields.add(new ParquetPartitionField(f));
      }
    }
    this.writeMode = m.writeMode;
    this.maxOpenPartitions = m.maxOpenPartitions;
  }

  /**
   * Combo text for {@link #version}. {@code GuiCompositeWidgets} shows enum {@code toString()}
   * values by default; this getter feeds the user-facing description instead.
   */
  public String getVersionDescription() {
    return version == null ? "" : version.getDescription();
  }

  /** Combo text for {@link #writeMode}. Same reason as {@link #getVersionDescription()}. */
  public String getWriteModeDescription() {
    return writeMode == null ? "" : writeMode.getDescription();
  }

  /**
   * Whether the transform partitions its output. Only then do the write mode and the maximum number
   * of open partitions have any effect.
   *
   * @return true if at least one partition field is configured
   */
  public boolean isPartitioning() {
    if (partitionFields == null) {
      return false;
    }
    for (ParquetPartitionField field : partitionFields) {
      if (field.getName() != null && !field.getName().trim().isEmpty()) {
        return true;
      }
    }
    return false;
  }
}
