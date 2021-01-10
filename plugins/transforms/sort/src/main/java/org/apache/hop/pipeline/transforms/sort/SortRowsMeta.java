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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.io.File;
import java.io.Serializable;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/*
 * Created on 02-jun-2003
 */
@InjectionSupported(
    localizationPrefix = "SortRows.Injection.",
    groups = {"FIELDS"})
@Transform(
    id = "SortRows",
    image = "sortrows.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SortRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.SortRows",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/sort.html")
public class SortRowsMeta extends BaseTransformMeta
    implements ITransformMeta<SortRows, SortRowsData>, Serializable {
  private static final long serialVersionUID = -9075883720765645655L;
  private static final Class<?> PKG = SortRowsMeta.class; // For Translator

  /** order by which fields? */
  @Injection(name = "NAME", group = "FIELDS")
  private String[] fieldName;

  /** false : descending, true=ascending */
  @Injection(name = "SORT_ASCENDING", group = "FIELDS")
  private boolean[] ascending;

  /** false : case insensitive, true=case sensitive */
  @Injection(name = "IGNORE_CASE", group = "FIELDS")
  private boolean[] caseSensitive;

  /** false : collator disabeld, true=collator enabled */
  @Injection(name = "COLLATOR_ENABLED", group = "FIELDS")
  private boolean[] collatorEnabled;

  // collator strength, 0,1,2,3
  @Injection(name = "COLLATOR_STRENGTH", group = "FIELDS")
  private int[] collatorStrength;

  /** false : not a presorted field, true=presorted field */
  @Injection(name = "PRESORTED", group = "FIELDS")
  private boolean[] preSortedField;

  private List<String> groupFields;

  /** Directory to store the temp files */
  @Injection(name = "SORT_DIRECTORY")
  private String directory;

  /** Temp files prefix... */
  @Injection(name = "SORT_FILE_PREFIX")
  private String prefix;

  /** The sort size: number of rows sorted and kept in memory */
  @Injection(name = "SORT_SIZE_ROWS")
  private String sortSize;

  /** The free memory limit in percentages in case we don't use the sort size */
  @Injection(name = "FREE_MEMORY_TRESHOLD")
  private String freeMemoryLimit;

  /** only pass unique rows to the output stream(s) */
  @Injection(name = "ONLY_PASS_UNIQUE_ROWS")
  private boolean onlyPassingUniqueRows;

  /**
   * Compress files: if set to true, temporary files are compressed, thus reducing I/O at the cost
   * of slightly higher CPU usage
   */
  @Injection(name = "COMPRESS_TEMP_FILES")
  private boolean compressFiles;

  /** The variable to use to set the compressFiles option boolean */
  private String compressFilesVariable;

  public SortRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the ascending. */
  public boolean[] getAscending() {
    return ascending;
  }

  /** @param ascending The ascending to set. */
  public void setAscending(boolean[] ascending) {
    this.ascending = ascending;
  }

  /** @return Returns the directory. */
  public String getDirectory() {
    return directory;
  }

  /** @param directory The directory to set. */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  /** @return Returns the fieldName. */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set. */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  /** @return Returns the prefix. */
  public String getPrefix() {
    return prefix;
  }

  /** @param prefix The prefix to set. */
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields]; // order by
    ascending = new boolean[nrFields];
    caseSensitive = new boolean[nrFields];
    collatorEnabled = new boolean[nrFields];
    collatorStrength = new int[nrFields];
    preSortedField = new boolean[nrFields];
    groupFields = null;
  }

  @Override
  public Object clone() {
    SortRowsMeta retval = (SortRowsMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate(nrFields);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    System.arraycopy(ascending, 0, retval.ascending, 0, nrFields);
    System.arraycopy(caseSensitive, 0, retval.caseSensitive, 0, nrFields);
    System.arraycopy(collatorEnabled, 0, retval.collatorEnabled, 0, nrFields);
    System.arraycopy(collatorStrength, 0, retval.collatorStrength, 0, nrFields);
    System.arraycopy(preSortedField, 0, retval.preSortedField, 0, nrFields);

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SortRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SortRows(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      directory = XmlHandler.getTagValue(transformNode, "directory");
      prefix = XmlHandler.getTagValue(transformNode, "prefix");
      sortSize = XmlHandler.getTagValue(transformNode, "sort_size");
      freeMemoryLimit = XmlHandler.getTagValue(transformNode, "free_memory");
      compressFiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "compress"));
      compressFilesVariable = XmlHandler.getTagValue(transformNode, "compress_variable");
      onlyPassingUniqueRows =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "unique_rows"));

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);
      String defaultStrength = Integer.toString(this.getDefaultCollationStrength());

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        String asc = XmlHandler.getTagValue(fnode, "ascending");
        ascending[i] = "Y".equalsIgnoreCase(asc);
        String sens = XmlHandler.getTagValue(fnode, "case_sensitive");
        String coll = Const.NVL(XmlHandler.getTagValue(fnode, "collator_enabled"), "N");
        caseSensitive[i] = Utils.isEmpty(sens) || "Y".equalsIgnoreCase(sens);
        collatorEnabled[i] = "Y".equalsIgnoreCase(coll);
        collatorStrength[i] =
            Integer.parseInt(
                Const.NVL(XmlHandler.getTagValue(fnode, "collator_strength"), defaultStrength));
        String presorted = XmlHandler.getTagValue(fnode, "presorted");
        preSortedField[i] = "Y".equalsIgnoreCase(presorted);
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
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

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      fieldName[i] = "field" + i;
      caseSensitive[i] = true;
      collatorEnabled[i] = false;
      collatorStrength[i] = 0;
      preSortedField[i] = false;
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(256);
    retval.append("      ").append(XmlHandler.addTagValue("directory", directory));
    retval.append("      ").append(XmlHandler.addTagValue("prefix", prefix));
    retval.append("      ").append(XmlHandler.addTagValue("sort_size", sortSize));
    retval.append("      ").append(XmlHandler.addTagValue("free_memory", freeMemoryLimit));
    retval.append("      ").append(XmlHandler.addTagValue("compress", compressFiles));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("compress_variable", compressFilesVariable));
    retval.append("      ").append(XmlHandler.addTagValue("unique_rows", onlyPassingUniqueRows));

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", fieldName[i]));
      retval.append("        ").append(XmlHandler.addTagValue("ascending", ascending[i]));
      retval.append("        ").append(XmlHandler.addTagValue("case_sensitive", caseSensitive[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("collator_enabled", collatorEnabled[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("collator_strength", collatorStrength[i]));
      retval.append("        ").append(XmlHandler.addTagValue("presorted", preSortedField[i]));
      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
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

  @SuppressWarnings("WeakerAccess")
  public void assignSortingCriteria(IRowMeta inputRowMeta) {
    for (int i = 0; i < fieldName.length; i++) {
      int idx = inputRowMeta.indexOfValue(fieldName[i]);
      if (idx >= 0) {
        IValueMeta valueMeta = inputRowMeta.getValueMeta(idx);
        // On all these valueMetas, check to see if the value actually exists before we try to
        // set them.
        if (ascending.length > i) {
          valueMeta.setSortedDescending(!ascending[i]);
        }
        if (caseSensitive.length > i) {
          valueMeta.setCaseInsensitive(!caseSensitive[i]);
        }
        if (collatorEnabled.length > i) {
          valueMeta.setCollatorDisabled(!collatorEnabled[i]);
        }
        if (collatorStrength.length > i) {
          valueMeta.setCollatorStrength(collatorStrength[i]);
        }
        // Also see if lazy conversion is active on these key fields.
        // If so we want to automatically convert them to the normal storage type.
        // This will improve performance, see also: PDI-346
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

    if (prev != null && prev.size() > 0) {
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
      for (int i = 0; i < fieldName.length; i++) {
        int idx = prev.indexOfValue(fieldName[i]);
        if (idx < 0) {
          errorMessage += "\t\t" + fieldName[i] + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "SortRowsMeta.CheckResult.SortKeysNotFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (fieldName.length > 0) {
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

  @Override
  public SortRowsData getTransformData() {
    return new SortRowsData();
  }

  /** @return Returns the sortSize. */
  public String getSortSize() {
    return sortSize;
  }

  /** @param sortSize The sortSize to set. */
  public void setSortSize(String sortSize) {
    this.sortSize = sortSize;
  }

  /** @return Returns whether temporary files should be compressed */
  public boolean getCompressFiles() {
    return compressFiles;
  }

  /** @param compressFiles Whether to compress temporary files created during sorting */
  public void setCompressFiles(boolean compressFiles) {
    this.compressFiles = compressFiles;
  }

  /** @return the onlyPassingUniqueRows */
  public boolean isOnlyPassingUniqueRows() {
    return onlyPassingUniqueRows;
  }

  /** @param onlyPassingUniqueRows the onlyPassingUniqueRows to set */
  public void setOnlyPassingUniqueRows(boolean onlyPassingUniqueRows) {
    this.onlyPassingUniqueRows = onlyPassingUniqueRows;
  }

  /** @return the compressFilesVariable */
  public String getCompressFilesVariable() {
    return compressFilesVariable;
  }

  /** @param compressFilesVariable the compressFilesVariable to set */
  public void setCompressFilesVariable(String compressFilesVariable) {
    this.compressFilesVariable = compressFilesVariable;
  }

  /** @return the caseSensitive */
  public boolean[] getCaseSensitive() {
    return caseSensitive;
  }

  /** @param caseSensitive the caseSensitive to set */
  public void setCaseSensitive(boolean[] caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  /** @return the collatorEnabled */
  public boolean[] getCollatorEnabled() {
    return collatorEnabled;
  }

  /** @param collatorEnabled the collatorEnabled to set */
  public void setCollatorEnabled(boolean[] collatorEnabled) {
    this.collatorEnabled = collatorEnabled;
  }

  /** @return the collatorStrength */
  public int[] getCollatorStrength() {
    return collatorStrength;
  }

  /** @param collatorStrength the collatorStrength to set */
  public void setCollatorStrength(int[] collatorStrength) {
    this.collatorStrength = collatorStrength;
  }

  /** @return the freeMemoryLimit */
  public String getFreeMemoryLimit() {
    return freeMemoryLimit;
  }

  /** @param freeMemoryLimit the freeMemoryLimit to set */
  public void setFreeMemoryLimit(String freeMemoryLimit) {
    this.freeMemoryLimit = freeMemoryLimit;
  }

  /** @return the preSortedField */
  public boolean[] getPreSortedField() {
    return preSortedField;
  }

  /** @param preSorted the preSorteField to set */
  public void setPreSortedField(boolean[] preSorted) {
    preSortedField = preSorted;
  }

  public List<String> getGroupFields() {
    if (this.groupFields == null) {
      for (int i = 0; i < preSortedField.length; i++) {
        if (preSortedField[i]) {
          if (groupFields == null) {
            groupFields = new ArrayList<>();
          }
          groupFields.add(this.fieldName[i]);
        }
      }
    }
    return groupFields;
  }

  public boolean isGroupSortEnabled() {
    return this.getGroupFields() != null;
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (fieldName == null) ? -1 : fieldName.length;
    if (nrFields <= 0) {
      return;
    }
    boolean[][] rtnBooleanArrays =
        Utils.normalizeArrays(nrFields, ascending, caseSensitive, collatorEnabled, preSortedField);
    ascending = rtnBooleanArrays[0];
    caseSensitive = rtnBooleanArrays[1];
    collatorEnabled = rtnBooleanArrays[2];
    preSortedField = rtnBooleanArrays[3];

    int[][] rtnIntArrays = Utils.normalizeArrays(nrFields, collatorStrength);
    collatorStrength = rtnIntArrays[0];
  }
}
