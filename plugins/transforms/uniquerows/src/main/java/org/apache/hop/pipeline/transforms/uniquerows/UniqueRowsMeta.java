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

package org.apache.hop.pipeline.transforms.uniquerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
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

/*
 * Created on 02-jun-2003
 *
 */

@Transform(
    id = "Unique",
    image = "uniquerows.svg",
    name = "i18n::BaseTransform.TypeLongDesc.UniqueRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.UniqueRows",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/uniquerows.html")
public class UniqueRowsMeta extends BaseTransformMeta
    implements ITransformMeta<UniqueRows, UniqueRowsData> {
  private static final Class<?> PKG = UniqueRowsMeta.class; // For Translator

  /** Indicate that we want to count the number of doubles */
  private boolean countRows;

  /** The fieldname that will contain the number of doubles */
  private String countField;

  /** The fields to compare for double, null means all */
  private String[] compareFields;

  /** The fields to compare for double, null means all */
  private boolean[] caseInsensitive;

  private boolean rejectDuplicateRow;
  private String errorDescription;

  public UniqueRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the countRows. */
  public boolean isCountRows() {
    return countRows;
  }

  /** @param countRows The countRows to set. */
  public void setCountRows(boolean countRows) {
    this.countRows = countRows;
  }

  /** @return Returns the countField. */
  public String getCountField() {
    return countField;
  }

  /** @param countField The countField to set. */
  public void setCountField(String countField) {
    this.countField = countField;
  }

  /** @param compareField The compareField to set. */
  public void setCompareFields(String[] compareField) {
    this.compareFields = compareField;
  }

  /** @return Returns the compareField. */
  public String[] getCompareFields() {
    return compareFields;
  }

  /** @param rejectDuplicateRow The rejectDuplicateRow to set. */
  public void setRejectDuplicateRow(boolean rejectDuplicateRow) {
    this.rejectDuplicateRow = rejectDuplicateRow;
  }

  /** @return Returns the rejectDuplicateRow. */
  public boolean isRejectDuplicateRow() {
    return rejectDuplicateRow;
  }

  public void allocate(int nrFields) {
    compareFields = new String[nrFields];
    caseInsensitive = new boolean[nrFields];
  }

  /** @return Returns the errorDescription. */
  public String getErrorDescription() {
    return errorDescription;
  }

  /** @param errorDescription The errorDescription to set. */
  public void setErrorDescription(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    UniqueRowsMeta retval = (UniqueRowsMeta) super.clone();

    int nrFields = compareFields.length;

    retval.allocate(nrFields);
    System.arraycopy(compareFields, 0, retval.compareFields, 0, nrFields);
    System.arraycopy(caseInsensitive, 0, retval.caseInsensitive, 0, nrFields);

    return retval;
  }

  @Override
  public UniqueRows createTransform(
      TransformMeta transformMeta,
      UniqueRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new UniqueRows(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      countRows = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "count_rows"));
      countField = XmlHandler.getTagValue(transformNode, "count_field");
      rejectDuplicateRow =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "reject_duplicate_row"));
      errorDescription = XmlHandler.getTagValue(transformNode, "error_description");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        compareFields[i] = XmlHandler.getTagValue(fnode, "name");
        caseInsensitive[i] =
            !"N".equalsIgnoreCase(XmlHandler.getTagValue(fnode, "case_insensitive"));
      }

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "UniqueRowsMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  @Override
  public void setDefault() {
    countRows = false;
    countField = "";
    rejectDuplicateRow = false;
    errorDescription = null;

    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      compareFields[i] = "field" + i;
      caseInsensitive[i] = true;
    }
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // change the case insensitive flag too
    for (int i = 0; i < compareFields.length; i++) {
      int idx = row.indexOfValue(compareFields[i]);
      if (idx >= 0) {
        row.getValueMeta(idx).setCaseInsensitive(caseInsensitive[i]);
      }
    }
    if (countRows) {
      IValueMeta v = new ValueMetaInteger(countField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("      " + XmlHandler.addTagValue("count_rows", countRows));
    retval.append("      " + XmlHandler.addTagValue("count_field", countField));
    retval.append("      " + XmlHandler.addTagValue("reject_duplicate_row", rejectDuplicateRow));
    retval.append("      " + XmlHandler.addTagValue("error_description", errorDescription));

    retval.append("    <fields>");
    for (int i = 0; i < compareFields.length; i++) {
      retval.append("      <field>");
      retval.append("        " + XmlHandler.addTagValue("name", compareFields[i]));
      retval.append("        " + XmlHandler.addTagValue("case_insensitive", caseInsensitive[i]));
      retval.append("        </field>");
    }
    retval.append("      </fields>");

    return retval.toString();
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

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "UniqueRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "UniqueRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public UniqueRowsData getTransformData() {
    return new UniqueRowsData();
  }

  /** @return Returns the caseInsensitive. */
  public boolean[] getCaseInsensitive() {
    return caseInsensitive;
  }

  /** @param caseInsensitive The caseInsensitive to set. */
  public void setCaseInsensitive(boolean[] caseInsensitive) {
    this.caseInsensitive = caseInsensitive;
  }

  @Override
  public boolean supportsErrorHandling() {
    return isRejectDuplicateRow();
  }
}
