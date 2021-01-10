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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "SplitFieldToRows3",
    name = "i18n::SplitFieldToRows.Name",
    image = "splitfieldtorows.svg",
    description = "i18n::SplitFieldToRows.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/splitfieldtorows.html")
public class SplitFieldToRowsMeta extends BaseTransformMeta
    implements ITransformMeta<SplitFieldToRows, SplitFieldToRowsData> {
  private static final Class<?> PKG = SplitFieldToRowsMeta.class; // For Translator

  /** Field to split */
  private String splitField;

  /** Split field based upon this delimiter. */
  private String delimiter;

  /** New name of the split field */
  private String newFieldname;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  /** Flag indicating that we should reset RowNum for each file */
  private boolean resetRowNumber;

  /** Flag indicating that the delimiter is a regular expression */
  private boolean isDelimiterRegex;

  public boolean isDelimiterRegex() {
    return isDelimiterRegex;
  }

  public void setDelimiterRegex(boolean isDelimiterRegex) {
    this.isDelimiterRegex = isDelimiterRegex;
  }

  public SplitFieldToRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the delimiter. */
  public String getDelimiter() {
    return delimiter;
  }

  /** @param delimiter The delimiter to set. */
  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  /** @return Returns the splitField. */
  public String getSplitField() {
    return splitField;
  }

  /** @param splitField The splitField to set. */
  public void setSplitField(String splitField) {
    this.splitField = splitField;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      splitField = XmlHandler.getTagValue(transformNode, "splitfield");
      delimiter = XmlHandler.getTagValue(transformNode, "delimiter");
      newFieldname = XmlHandler.getTagValue(transformNode, "newfield");
      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      resetRowNumber =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "resetrownumber"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      isDelimiterRegex =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "delimiter_is_regex"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "SplitFieldToRowsMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    splitField = "";
    delimiter = ";";
    newFieldname = "";
    includeRowNumber = false;
    isDelimiterRegex = false;
    rowNumberField = "";
    resetRowNumber = true;
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    IValueMeta v = new ValueMetaString(newFieldname);
    v.setOrigin(name);
    row.addValueMeta(v);

    // include row number
    if (includeRowNumber) {
      v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("   " + XmlHandler.addTagValue("splitfield", splitField));
    retval.append("   " + XmlHandler.addTagValue("delimiter", delimiter));
    retval.append("   " + XmlHandler.addTagValue("newfield", newFieldname));
    retval.append("   " + XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("   " + XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval.append("   " + XmlHandler.addTagValue("resetrownumber", resetRowNumber));
    retval.append("   " + XmlHandler.addTagValue("delimiter_is_regex", isDelimiterRegex));

    return retval.toString();
  }

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
    String errorMessage = "";
    CheckResult cr;

    // Look up fields in the input stream <prev>
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "SplitFieldToRowsMeta.CheckResult.TransformReceivingFields",
                  prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      errorMessage = "";

      IValueMeta v = prev.searchValueMeta(splitField);
      if (v == null) {
        errorMessage =
            BaseMessages.getString(
                PKG,
                "SplitFieldToRowsMeta.CheckResult.FieldToSplitNotPresentInInputStream",
                splitField);
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "SplitFieldToRowsMeta.CheckResult.FieldToSplitFoundInInputStream",
                    splitField),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      errorMessage =
          BaseMessages.getString(
                  PKG, "SplitFieldToRowsMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform")
              + Const.CR;
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SplitFieldToRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransform"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SplitFieldToRowsMeta.CheckResult.NoInputReceivedFromOtherTransform"),
              transformMeta);
      remarks.add(cr);
    }

    if (Utils.isEmpty(newFieldname)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SplitFieldToRowsMeta.CheckResult.NewFieldNameIsNull"),
              transformMeta);
      remarks.add(cr);
    }
    if (includeRowNumber) {
      if (Utils.isEmpty(variables.resolve(rowNumberField))) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "SplitFieldToRowsMeta.CheckResult.RowNumberFieldMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "SplitFieldToRowsMeta.CheckResult.RowNumberFieldOk"),
                transformMeta);
      }
      remarks.add(cr);
    }
  }

  @Override
  public SplitFieldToRows createTransform(
      TransformMeta transformMeta,
      SplitFieldToRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SplitFieldToRows(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public SplitFieldToRowsData getTransformData() {
    return new SplitFieldToRowsData();
  }

  /** @return the newFieldname */
  public String getNewFieldname() {
    return newFieldname;
  }

  /** @param newFieldname the newFieldname to set */
  public void setNewFieldname(String newFieldname) {
    this.newFieldname = newFieldname;
  }

  /** @return Returns the rowNumberField. */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /** @return Returns the resetRowNumber. */
  public boolean resetRowNumber() {
    return resetRowNumber;
  }

  /** @param resetRowNumber The resetRowNumber to set. */
  public void setResetRowNumber(boolean resetRowNumber) {
    this.resetRowNumber = resetRowNumber;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /** @return Returns the includeRowNumber. */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }
}
