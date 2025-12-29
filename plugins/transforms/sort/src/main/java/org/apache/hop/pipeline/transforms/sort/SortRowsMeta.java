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

package org.apache.hop.pipeline.transforms.sort;

import java.io.File;
import java.io.Serializable;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SortRows",
    image = "sortrows.svg",
    name = "i18n::SortRows.Name",
    description = "i18n::SortRows.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::SortRowsMeta.keyword",
    documentationUrl = "/pipeline/transforms/sort.html")
public class SortRowsMeta extends BaseTransformMeta<SortRows, SortRowsData>
    implements Serializable {
  private static final long serialVersionUID = -9075883720765645655L;
  private static final Class<?> PKG = SortRowsMeta.class;
  private static final String CONST_SPACE = "      ";
  private static final String CONST_SPACE_LONG = "        ";
  private static final String CONST_FIELD = "field";

  @HopMetadataProperty(groupKey = "fields", key = "field", injectionGroupKey = "FIELDS")
  private List<SortRowsField> sortFields = new ArrayList<>();

  /** Directory to store the temp files */
  @HopMetadataProperty(key = "directory", injectionKey = "SORT_DIRECTORY")
  private String directory;

  /** Temp files prefix... */
  @HopMetadataProperty(key = "sort_prefix", injectionKey = "SORT_FILE_PREFIX")
  private String prefix;

  /** The sort size: number of rows sorted and kept in memory */
  @HopMetadataProperty(key = "sort_size", injectionKey = "SORT_SIZE_ROWS")
  private String sortSize;

  /**
   * The free memory limit in percentages in case we don't use the sort size We need the keep the
   * missing 'H' in FREE_MEMORY_TRESHOLD for backwards compatibility.
   */
  @HopMetadataProperty(key = "free_memory", injectionKey = "FREE_MEMORY_TRESHOLD")
  private String freeMemoryLimit;

  /** only pass unique rows to the output stream(s) */
  @HopMetadataProperty(key = "unique_rows", injectionKey = "ONLY_PASS_UNIQUE_ROWS")
  private boolean onlyPassingUniqueRows;

  /**
   * Compress files: if set to true, temporary files are compressed, thus reducing I/O at the cost
   * of slightly higher CPU usage
   */
  @HopMetadataProperty(key = "compress", injectionKey = "COMPRESS_TEMP_FILES")
  private boolean compressFiles;

  /** The variable to use to set the compressFiles option boolean */
  @HopMetadataProperty(key = "compress_variables", injectionKey = "COMPRESS_VARIABLE")
  private String compressFilesVariable;

  private List<SortRowsField> groupFields;

  public SortRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the directory.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  /**
   * @return Returns the prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @param prefix The prefix to set.
   */
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public List<SortRowsField> getSortFields() {
    return sortFields;
  }

  public void setSortFields(List<SortRowsField> sortFields) {
    this.sortFields = sortFields;
  }

  @Override
  public Object clone() {
    SortRowsMeta retval = (SortRowsMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    directory = "${java.io.tmpdir}";
    prefix = "out";
    sortSize = "1000000";
    freeMemoryLimit = null;
    compressFiles = false;
    compressFilesVariable = null;
    onlyPassingUniqueRows = false;

    int nrFields = 0;
  }

  // Returns the default collation strength based on the users' default locale.
  // Package protected for testing purposes
  int getDefaultCollationStrength() {
    return getDefaultCollationStrength(Locale.getDefault());
  }

  // Returns the collation strength based on the passed in locale.
  // Package protected for testing purposes
  int getDefaultCollationStrength(Locale aLocale) {
    int defaultStrength = Collator.IDENTICAL;
    if (aLocale != null) {
      Collator curDefCollator = Collator.getInstance(aLocale);
      if (curDefCollator != null) {
        defaultStrength = curDefCollator.getStrength();
      }
    }
    return defaultStrength;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Set the sorted properties: ascending/descending
    assignSortingCriteria(inputRowMeta);
  }

  public void assignSortingCriteria(IRowMeta inputRowMeta) {
    for (SortRowsField field : sortFields) {
      int idx = inputRowMeta.indexOfValue(field.getFieldName());
      if (idx >= 0) {
        IValueMeta valueMeta = inputRowMeta.getValueMeta(idx);
        valueMeta.setSortedDescending(!field.isAscending());
        valueMeta.setCaseInsensitive(!field.isCaseSensitive());
        valueMeta.setCollatorDisabled(!field.isCollatorEnabled());
        valueMeta.setCollatorStrength(field.getCollatorStrength());
        // Also see if lazy conversion is active on these key fields.
        // If so we want to automatically convert them to the normal storage type.
        // This will improve performance
        //
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        valueMeta.setStorageMetadata(null);
      }
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SortRowsMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (SortRowsField field : sortFields) {
        int idx = prev.indexOfValue(field.getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getFieldName() + Const.CR;
          errorFound = true;
        }
      }

      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.SortKeysNotFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (!sortFields.isEmpty()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.AllSortKeysFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.NoSortKeysEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }

      // Check the sort directory
      String realDirectory = variables.resolve(directory);

      File f = new File(realDirectory);
      if (f.exists()) {
        if (f.isDirectory()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "SortRowsMeta.CheckResult.DirectoryExists", realDirectory),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "SortRowsMeta.CheckResult.ExistsButNoDirectory", realDirectory),
                  transformMeta);
          remarks.add(cr);
        }
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "SortRowsMeta.CheckResult.DirectoryNotExists", realDirectory),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * @return Returns the sortSize.
   */
  public String getSortSize() {
    return sortSize;
  }

  /**
   * @param sortSize The sortSize to set.
   */
  public void setSortSize(String sortSize) {
    this.sortSize = sortSize;
  }

  /**
   * @return Returns whether temporary files should be compressed
   */
  public boolean isCompressFiles() {
    return compressFiles;
  }

  /**
   * @param compressFiles Whether to compress temporary files created during sorting
   */
  public void setCompressFiles(boolean compressFiles) {
    this.compressFiles = compressFiles;
  }

  /**
   * @return the onlyPassingUniqueRows
   */
  public boolean isOnlyPassingUniqueRows() {
    return onlyPassingUniqueRows;
  }

  /**
   * @param onlyPassingUniqueRows the onlyPassingUniqueRows to set
   */
  public void setOnlyPassingUniqueRows(boolean onlyPassingUniqueRows) {
    this.onlyPassingUniqueRows = onlyPassingUniqueRows;
  }

  /**
   * @return the compressFilesVariable
   */
  public String getCompressFilesVariable() {
    return compressFilesVariable;
  }

  /**
   * @param compressFilesVariable the compressFilesVariable to set
   */
  public void setCompressFilesVariable(String compressFilesVariable) {
    this.compressFilesVariable = compressFilesVariable;
  }

  /**
   * @return the freeMemoryLimit
   */
  public String getFreeMemoryLimit() {
    return freeMemoryLimit;
  }

  /**
   * @param freeMemoryLimit the freeMemoryLimit to set
   */
  public void setFreeMemoryLimit(String freeMemoryLimit) {
    this.freeMemoryLimit = freeMemoryLimit;
  }

  public boolean isGroupSortEnabled() {
    return this.getSortFields() != null;
  }

  public void setGroupFields(List<SortRowsField> groupFields) {
    this.groupFields = groupFields;
  }

  public List<SortRowsField> getGroupFields() {
    if (this.groupFields == null) {
      groupFields = new ArrayList<>();
      for (int i = 0; i < getSortFields().size(); i++) {
        if (getSortFields().get(i).isPreSortedField()) {
          //          if (groupFields == null) {
          //            groupFields = new ArrayList<>();
          //          }
          groupFields.add(getSortFields().get(i));
        }
      }
    }
    return groupFields;
  }
}
