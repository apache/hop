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

package org.apache.hop.pipeline.transforms.salesforceupdate;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;
import org.apache.hop.pipeline.transforms.salesforceinsert.SalesforceInsertField;

@Transform(
    id = "SalesforceUpdate",
    name = "i18n::SalesforceUpdate.TypeLongDesc.SalesforceUpdate",
    description = "i18n::SalesforceUpdate.TypeTooltipDesc.SalesforceUpdate",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::SalesforceUpdateMeta.keyword",
    image = "SFUD.svg",
    documentationUrl = "/pipeline/transforms/salesforceupdate.html")
@Getter
@Setter
public class SalesforceUpdateMeta
    extends SalesforceTransformMeta<SalesforceUpdate, SalesforceUpdateData> {
  private static final Class<?> PKG = SalesforceUpdateMeta.class;
  public static final String CONST_SPACES = "        ";
  public static final String CONST_FIELD = "field";

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<SalesforceInsertField> fields;

  /** Batch size */
  @HopMetadataProperty private String batchSize;

  @HopMetadataProperty private boolean rollbackAllChangesOnError;

  public SalesforceUpdateMeta() {
    super(); // allocate BaseTransformMeta
  }

  public int getBatchSizeInt() {
    return Const.toInt(this.batchSize, 10);
  }

  @Override
  public Object clone() {
    SalesforceUpdateMeta retval = (SalesforceUpdateMeta) super.clone();

    // Initialize the fields list
    retval.fields = new ArrayList<>();

    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i) != null) {
        //        retval.inputFields.get(i) = (SalesforceInputField) inputFields.get(i).clone();
        retval.fields.add((SalesforceInsertField) fields.get(i).clone());
      }
    }

    return retval;
  }

  @Override
  public void setDefault() {
    super.setDefault();
    setFields(new ArrayList<>());
    setBatchSize("10");

    setRollbackAllChangesOnError(false);
  }

  /* This function adds meta data to the rows being pushed out */
  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Do Nothing
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
    super.check(
        remarks,
        pipelineMeta,
        transformMeta,
        prev,
        input,
        output,
        info,
        variables,
        metadataProvider);

    CheckResult cr;

    // See if we get input...
    if (input != null && input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceUpdateMeta.CheckResult.NoInputExpected"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceUpdateMeta.CheckResult.NoInput"),
              transformMeta);
    }
    remarks.add(cr);

    // check return fields
    if (getFields().size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceUpdateMeta.CheckResult.NoFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceUpdateMeta.CheckResult.FieldsOk"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
