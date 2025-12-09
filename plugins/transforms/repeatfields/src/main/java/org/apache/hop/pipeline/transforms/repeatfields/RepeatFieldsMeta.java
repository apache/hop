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
 *
 */

package org.apache.hop.pipeline.transforms.repeatfields;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "RepeatFields",
    image = "repeatfields.svg",
    name = "i18n::RepeatFields.Name",
    description = "i18n::RepeatFields.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::RepeatFields.keyword",
    documentationUrl = "/pipeline/transforms/repeatfields.html")
@Getter
@Setter
public class RepeatFieldsMeta extends BaseTransformMeta<RepeatFields, RepeatFieldsData> {
  private static final Class<?> PKG = RepeatFields.class;

  @HopMetadataProperty(
      key = "group_field",
      groupKey = "group_fields",
      injectionGroupKey = "GROUP_FIELDS",
      injectionGroupDescription = "RepeatFields.Injection.GroupFields",
      injectionKey = "GROUP_FIELD",
      injectionKeyDescription = "RepeatFields.Injection.GroupField")
  private List<String> groupFields;

  @HopMetadataProperty(
      key = "field",
      groupKey = "repeats",
      injectionGroupKey = "REPEATS",
      injectionGroupDescription = "RepeatFields.Injection.Repeats",
      injectionKey = "REPEAT",
      injectionKeyDescription = "RepeatFields.Injection.Repeat")
  private List<Repeat> repeats;

  public RepeatFieldsMeta() {
    super();
    this.groupFields = new ArrayList<>();
    this.repeats = new ArrayList<>();
  }

  public RepeatFieldsMeta(RepeatFieldsMeta m) {
    this();
    this.groupFields.addAll(m.groupFields);
    for (Repeat repeat : m.repeats) {
      this.repeats.add(new Repeat(repeat));
    }
  }

  @Override
  public Object clone() {
    return new RepeatFieldsMeta(this);
  }

  /**
   * We only add an optional target field containing a corrected copy of the source field.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output
   *     row metadata of the transform
   * @param name Name of the transform to use as input for the origin field in the values
   * @param info Fields used as extra lookup information
   * @param nextTransform the next transform that is targeted
   * @param variables the variables The variable variables to use to replace variables
   * @param metadataProvider the MetaStore to use to load additional external data or metadata
   *     impacting the output fields
   * @throws HopTransformException in case there is a missing source field
   */
  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (Repeat repeat : repeats) {
      String targetFieldName = variables.resolve(repeat.getTargetField());
      if (StringUtils.isNotEmpty(targetFieldName)) {
        // We need to add a new field with the new name and the same data type as the source.
        String sourceFieldName = variables.resolve(repeat.getSourceField());
        IValueMeta sourceFieldValueMeta = inputRowMeta.searchValueMeta(sourceFieldName);
        if (sourceFieldValueMeta == null) {
          throw new HopTransformException(
              "Unable to find source field "
                  + sourceFieldName
                  + " in the input of transform "
                  + name);
        }
        IValueMeta targetFieldValueMeta = sourceFieldValueMeta.clone();
        targetFieldValueMeta.setName(targetFieldName);
        targetFieldValueMeta.setOrigin(name);
        inputRowMeta.addValueMeta(targetFieldValueMeta);
      }
    }
  }

  @Getter
  public enum RepeatType implements IEnumHasCodeAndDescription {
    Previous("previous", BaseMessages.getString(PKG, "RepeatFields.Previous.Description")),
    PreviousWhenNull(
        "previous_when_null",
        BaseMessages.getString(PKG, "RepeatFields.PreviousWhenNull.Description")),
    CurrentWhenIndicated(
        "current_when_indicated",
        BaseMessages.getString(PKG, "RepeatFields.CurrentWhenIndicated.Description"));

    private final String code;
    private final String description;

    RepeatType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static RepeatType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(RepeatType.class, description, null);
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(RepeatType.class);
    }
  }
}
