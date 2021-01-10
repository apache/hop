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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
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

import java.util.List;

@Transform(
    id = "UniqueRowsByHashSet",
    image = "uniquerowsbyhashset.svg",
    name = "i18n::BaseTransform.TypeLongDesc.UniqueRowsByHashSet",
    description = "i18n::BaseTransform.TypeTooltipDesc.UniqueRowsByHashSet",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/uniquerowsbyhashset.html")
public class UniqueRowsByHashSetMeta extends BaseTransformMeta
    implements ITransformMeta<UniqueRowsByHashSet, UniqueRowsByHashSetData> {
  private static final Class<?> PKG = UniqueRowsByHashSetMeta.class; // For Translator

  /**
   * Whether to compare strictly by hash value or to store the row values for strict equality
   * checking
   */
  private boolean storeValues;

  /** The fields to compare for duplicates, null means all */
  private String[] compareFields;

  private boolean rejectDuplicateRow;
  private String errorDescription;

  public UniqueRowsByHashSetMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @param compareField The compareField to set. */
  public void setCompareFields(String[] compareField) {
    this.compareFields = compareField;
  }

  public boolean getStoreValues() {
    return storeValues;
  }

  public void setStoreValues(boolean storeValues) {
    this.storeValues = storeValues;
  }

  /** @return Returns the compareField. */
  public String[] getCompareFields() {
    return compareFields;
  }

  public void allocate(int nrFields) {
    compareFields = new String[nrFields];
  }

  /** @param rejectDuplicateRow The rejectDuplicateRow to set. */
  public void setRejectDuplicateRow(boolean rejectDuplicateRow) {
    this.rejectDuplicateRow = rejectDuplicateRow;
  }

  /** @return Returns the rejectDuplicateRow. */
  public boolean isRejectDuplicateRow() {
    return rejectDuplicateRow;
  }

  /** @param errorDescription The errorDescription to set. */
  public void setErrorDescription(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  /** @return Returns the errorDescription. */
  public String getErrorDescription() {
    return errorDescription;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    UniqueRowsByHashSetMeta retval = (UniqueRowsByHashSetMeta) super.clone();

    int nrFields = compareFields.length;

    retval.allocate(nrFields);

    System.arraycopy(compareFields, 0, retval.compareFields, 0, nrFields);
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      UniqueRowsByHashSetData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new UniqueRowsByHashSet(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      storeValues = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "store_values"));
      rejectDuplicateRow =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "reject_duplicate_row"));
      errorDescription = XmlHandler.getTagValue(transformNode, "error_description");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        compareFields[i] = XmlHandler.getTagValue(fnode, "name");
      }

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "UniqueRowsByHashSetMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    rejectDuplicateRow = false;
    errorDescription = null;
    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      compareFields[i] = "field" + i;
    }
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {}

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("      " + XmlHandler.addTagValue("store_values", storeValues));
    retval.append("      " + XmlHandler.addTagValue("reject_duplicate_row", rejectDuplicateRow));
    retval.append("      " + XmlHandler.addTagValue("error_description", errorDescription));
    retval.append("    <fields>");
    for (int i = 0; i < compareFields.length; i++) {
      retval.append("      <field>");
      retval.append("        " + XmlHandler.addTagValue("name", compareFields[i]));
      retval.append("        </field>");
    }
    retval.append("      </fields>");

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
    CheckResult cr;

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "UniqueRowsByHashSetMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "UniqueRowsByHashSetMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public UniqueRowsByHashSetData getTransformData() {
    return new UniqueRowsByHashSetData();
  }

  public boolean supportsErrorHandling() {
    return isRejectDuplicateRow();
  }
}
