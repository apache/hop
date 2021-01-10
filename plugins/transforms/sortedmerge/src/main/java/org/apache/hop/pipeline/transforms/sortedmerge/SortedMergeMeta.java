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

package org.apache.hop.pipeline.transforms.sortedmerge;

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
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */
@InjectionSupported(
    localizationPrefix = "SortedMerge.Injection.",
    groups = {"FIELDS"})
@Transform(
    id = "SortedMerge",
    image = "sortedmerge.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SortedMerge",
    description = "i18n::BaseTransform.TypeTooltipDesc.SortedMerge",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/sortedmerge.html")
public class SortedMergeMeta extends BaseTransformMeta
    implements ITransformMeta<SortedMerge, SortedMergeData> {
  private static final Class<?> PKG = SortedMergeMeta.class; // For Translator

  /** order by which fields? */
  @Injection(name = "FIELD_NAME", group = "FIELDS")
  private String[] fieldName;
  /** false : descending, true=ascending */
  @Injection(name = "ASCENDING", group = "FIELDS")
  private boolean[] ascending;

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    fieldName = new String[nrFields]; // order by
    ascending = new boolean[nrFields];
  }

  public void setDefault() {
    int nrFields = 0;

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      fieldName[i] = "field" + i;
    }
  }

  public Object clone() {
    SortedMergeMeta retval = (SortedMergeMeta) super.clone();

    int nrFields = fieldName.length;

    retval.allocate(nrFields);
    System.arraycopy(fieldName, 0, retval.fieldName, 0, nrFields);
    System.arraycopy(ascending, 0, retval.ascending, 0, nrFields);

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SortedMergeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SortedMerge(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        fieldName[i] = XmlHandler.getTagValue(fnode, "name");
        String asc = XmlHandler.getTagValue(fnode, "ascending");
        if (asc.equalsIgnoreCase("Y")) {
          ascending[i] = true;
        } else {
          ascending[i] = false;
        }
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    <fields>" + Const.CR);
    for (int i = 0; i < fieldName.length; i++) {
      retval.append("      <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", fieldName[i]));
      retval.append("        " + XmlHandler.addTagValue("ascending", ascending[i]));
      retval.append("        </field>" + Const.CR);
    }
    retval.append("      </fields>" + Const.CR);

    return retval.toString();
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Set the sorted properties: ascending/descending
    for (int i = 0; i < fieldName.length; i++) {
      int idx = inputRowMeta.indexOfValue(fieldName[i]);
      if (idx >= 0) {
        IValueMeta valueMeta = inputRowMeta.getValueMeta(idx);
        valueMeta.setSortedDescending(!ascending[i]);

        // TODO: add case insensivity
      }
    }
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

    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SortedMergeMeta.CheckResult.FieldsReceived", "" + prev.size()),
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
            BaseMessages.getString(
                PKG, "SortedMergeMeta.CheckResult.SortKeysNotFound", errorMessage);

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        if (fieldName.length > 0) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.AllSortKeysFound"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.NoSortKeysEntered"),
                  transformMeta);
          remarks.add(cr);
        }
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedMergeMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public SortedMergeData getTransformData() {
    return new SortedMergeData();
  }

  /** @return the ascending */
  public boolean[] getAscending() {
    return ascending;
  }

  /** @param ascending the ascending to set */
  public void setAscending(boolean[] ascending) {
    this.ascending = ascending;
  }

  /** @return the fieldName */
  public String[] getFieldName() {
    return fieldName;
  }

  /** @param fieldName the fieldName to set */
  public void setFieldName(String[] fieldName) {
    this.fieldName = fieldName;
  }

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
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
    boolean[][] rtn = Utils.normalizeArrays(nrFields, ascending);
    ascending = rtn[0];
  }
}
