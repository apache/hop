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
 *
 */

package org.apache.hop.pipeline.transforms.sortedschemamerge;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SortedSchemaMerge",
    image = "sortedschemamerge.svg",
    name = "i18n::SortedSchemaMerge.Name",
    description = "i18n::SortedSchemaMerge.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::SortedSchemaMerge.keyword",
    documentationUrl = "/pipeline/transforms/sortedschemamerge.html")
@Getter
@Setter
public class SortedSchemaMergeMeta
    extends BaseTransformMeta<SortedSchemaMerge, SortedSchemaMergeData> {

  private static final Class<?> PKG = SortedSchemaMergeMeta.class;

  @HopMetadataProperty(key = "input", groupKey = "inputs")
  private List<SortedSchemaMergeInput> inputs = new ArrayList<>();

  @HopMetadataProperty(key = "sort_key", groupKey = "sort_keys")
  private List<SortedSchemaMergeSortKey> sortKeys = new ArrayList<>();

  public SortedSchemaMergeMeta() {}

  public SortedSchemaMergeMeta(SortedSchemaMergeMeta other) {
    if (other == null) {
      return;
    }
    for (SortedSchemaMergeInput input : other.inputs) {
      inputs.add(new SortedSchemaMergeInput(input));
    }
    for (SortedSchemaMergeSortKey sortKey : other.sortKeys) {
      sortKeys.add(new SortedSchemaMergeSortKey(sortKey));
    }
  }

  @Override
  public SortedSchemaMergeMeta clone() {
    return new SortedSchemaMergeMeta(this);
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public void getFields(
      PipelineMeta pipelineMeta,
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    IRowMeta[] inputLayouts = resolveInputLayouts(pipelineMeta, name, info, variables);
    if (inputLayouts == null || inputLayouts.length == 0) {
      return;
    }
    mergeOutputFields(inputRowMeta, name, inputLayouts);
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
    IRowMeta[] inputLayouts = resolveInputLayouts(null, name, info, variables);
    if (inputLayouts == null || inputLayouts.length == 0) {
      return;
    }
    mergeOutputFields(inputRowMeta, name, inputLayouts);
  }

  private static IRowMeta[] resolveInputLayouts(
      PipelineMeta pipelineMeta, String transformName, IRowMeta[] info, IVariables variables)
      throws HopTransformException {
    if (info != null && info.length > 0 && !isEmptyLayout(info)) {
      return info;
    }
    if (pipelineMeta == null) {
      return info;
    }
    TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
    if (transformMeta == null) {
      return info;
    }
    String[] inputNames = pipelineMeta.getPrevTransformNames(transformMeta);
    if (inputNames == null || inputNames.length == 0) {
      return info;
    }
    List<IRowMeta> layouts = new ArrayList<>();
    for (String inputName : inputNames) {
      if (Utils.isEmpty(inputName)) {
        continue;
      }
      IRowMeta layout = pipelineMeta.getTransformFields(variables, inputName);
      if (layout != null && !layout.isEmpty()) {
        layouts.add(layout);
      }
    }
    return layouts.isEmpty() ? info : layouts.toArray(new IRowMeta[0]);
  }

  private static boolean isEmptyLayout(IRowMeta[] layouts) {
    for (IRowMeta layout : layouts) {
      if (layout != null && !layout.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private static void mergeOutputFields(IRowMeta inputRowMeta, String name, IRowMeta[] inputLayouts)
      throws HopTransformException {
    try {
      SortedSchemaMergeLogic.SchemaMapping mapping =
          SortedSchemaMergeLogic.buildSchemaMapping(inputLayouts);
      IRowMeta outputRowMeta = mapping.getOutputRowMeta();
      for (int i = 0; i < outputRowMeta.size(); i++) {
        outputRowMeta.getValueMeta(i).setOrigin(name);
      }
      inputRowMeta.mergeRowMeta(outputRowMeta);
    } catch (HopPluginException e) {
      throw new HopTransformException("Unable to resolve merged output fields", e);
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
    if (inputs == null || inputs.size() < 2) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedSchemaMergeMeta.CheckResult.TooFewInputs"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SortedSchemaMergeMeta.CheckResult.InputCountOk"),
              transformMeta));
    }

    if (sortKeys == null || sortKeys.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedSchemaMergeMeta.CheckResult.NoSortKeys"),
              transformMeta));
      return;
    }

    List<IRowMeta> inputLayouts = resolveCheckInputLayouts(pipelineMeta, input, prev, variables);
    if (!inputLayouts.isEmpty()) {
      StringBuilder missing = new StringBuilder();
      for (SortedSchemaMergeSortKey sortKey : sortKeys) {
        if (sortKey == null || Utils.isEmpty(sortKey.getFieldName())) {
          continue;
        }
        for (IRowMeta rowMeta : inputLayouts) {
          if (rowMeta != null && rowMeta.indexOfValue(sortKey.getFieldName()) < 0) {
            missing.append("\t\t").append(sortKey.getFieldName()).append(Const.CR);
            break;
          }
        }
      }
      if (!missing.isEmpty()) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "SortedSchemaMergeMeta.CheckResult.SortKeysMissing", missing.toString()),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "SortedSchemaMergeMeta.CheckResult.SortKeysOk"),
                transformMeta));
      }
    }

    if (input != null && input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SortedSchemaMergeMeta.CheckResult.ReceivingRowsOk"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SortedSchemaMergeMeta.CheckResult.ReceivingRowsError"),
              transformMeta));
    }
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {PipelineType.Normal};
  }

  private static List<IRowMeta> resolveCheckInputLayouts(
      PipelineMeta pipelineMeta, String[] input, IRowMeta prev, IVariables variables) {
    List<IRowMeta> layouts = new ArrayList<>();
    if (input != null) {
      for (String inputName : input) {
        if (Utils.isEmpty(inputName)) {
          continue;
        }
        try {
          IRowMeta layout = pipelineMeta.getTransformFields(variables, inputName);
          if (layout != null && !layout.isEmpty()) {
            layouts.add(layout);
          }
        } catch (HopTransformException ignored) {
          // Pipeline metadata may be incomplete while the dialog is being edited.
        }
      }
    }
    if (layouts.isEmpty() && prev != null && !prev.isEmpty()) {
      layouts.add(prev);
    }
    return layouts;
  }
}
