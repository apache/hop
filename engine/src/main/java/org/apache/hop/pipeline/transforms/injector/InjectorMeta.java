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

package org.apache.hop.pipeline.transforms.injector;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import java.util.ArrayList;
import java.util.List;

// TODO: check conversion of types from strings to numbers and back.
//       As compared in the old version.

/**
 * Metadata class to allow a java program to inject rows of data into a pipeline. This transform can
 * be used as a starting point in such a "headless" pipeline.
 *
 * @since 22-jun-2006
 */
@Transform(
    id = "Injector",
    image = "ui/images/injector.svg",
    name = "i18n:org.apache.hop.pipeline.transform:BaseTransform.TypeLongDesc.Injector",
    description = "i18n:org.apache.hop.pipeline.transform:BaseTransform.TypeTooltipDesc.Injector",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Inline",
    keywords = "",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/injector.html")
public class InjectorMeta extends BaseTransformMeta<Injector, InjectorData>
{

  private static final Class<?> PKG = InjectorMeta.class; // For Translator

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<InjectorField> injectorFields;

  public InjectorMeta() {
    injectorFields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
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
    for (InjectorField field : injectorFields) {
      try {
        inputRowMeta.addValueMeta(field.createValueMeta(variables));
      } catch (HopException e) {
        throw new HopTransformException(e);
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
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "InjectorMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "InjectorMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * Gets injectorFields
   *
   * @return value of injectorFields
   */
  public List<InjectorField> getInjectorFields() {
    return injectorFields;
  }

  /** @param injectorFields The injectorFields to set */
  public void setInjectorFields(List<InjectorField> injectorFields) {
    this.injectorFields = injectorFields;
  }
}
