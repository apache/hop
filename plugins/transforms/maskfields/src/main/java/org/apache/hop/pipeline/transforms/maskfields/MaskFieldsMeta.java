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

package org.apache.hop.pipeline.transforms.maskfields;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.maskfields.store.DatabaseMaskingStore;

@Getter
@Setter
@GuiPlugin
@Transform(
    id = "MaskFields",
    image = "maskfields.svg",
    name = "i18n::MaskFields.Name",
    description = "i18n::MaskFields.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::MaskFieldsMeta.keyword",
    documentationUrl = "/pipeline/transforms/maskfields.html",
    excludedEngines = {"Beam*", "SparkPipelineEngine"})
public class MaskFieldsMeta extends BaseTransformMeta<MaskFields, MaskFieldsData> {

  private static final Class<?> PKG = MaskFieldsMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "MaskFields.Dialog";
  public static final String WIDGET_EDIT_RULE = "editRule";
  public static final String WIDGET_FIELDS = "fields";

  /** Column index of the masking rule in the fields table. 0 is the row number. */
  public static final int RULE_COLUMN = 2;

  /** Longest token a pattern can write: a UUID, or the digits of the largest sequence value. */
  private static final int UUID_LENGTH = 36;

  private static final int LONG_DIGITS = Long.toString(Long.MAX_VALUE).length();

  @GuiWidgetElement(
      id = WIDGET_EDIT_RULE,
      order = "0200",
      type = GuiElementType.BUTTON,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::MaskFields.Group.Fields",
      label = "i18n::MaskFields.EditRule.Label",
      toolTip = "i18n::MaskFields.EditRule.Tooltip")
  public void editMaskingRule(Object sourceObject) {
    // The dialog edits the selected rule, creates one, or opens the metadata type.
  }

  @HopMetadataProperty(key = "field", groupKey = "fields")
  @GuiWidgetElement(
      id = WIDGET_FIELDS,
      order = "0300",
      type = GuiElementType.TABLE,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::MaskFields.Group.Fields",
      toolTip = "i18n::MaskFields.Fields.Tooltip",
      tableRows = 8)
  private List<MaskField> fields = new ArrayList<>();

  public MaskFieldsMeta() {
    this.fields = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    fields = new ArrayList<>();
  }

  /** Pattern names for the grid combo. */
  public List<String> patternNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    if (metadataProvider == null) {
      return List.of();
    }
    try {
      List<String> names = new ArrayList<>(serializer(metadataProvider).listObjectNames());
      Collections.sort(names);
      return names;
    } catch (Exception e) {
      if (log != null) {
        log.logError("Unable to list masking patterns", e);
      }
      return List.of();
    }
  }

  public MaskingPattern loadPattern(IHopMetadataProvider metadataProvider, String name)
      throws HopException {
    if (metadataProvider == null || StringUtils.isEmpty(name)) {
      return null;
    }
    return serializer(metadataProvider).load(name);
  }

  private IHopMetadataSerializer<MaskingPattern> serializer(IHopMetadataProvider metadataProvider)
      throws HopException {
    return metadataProvider.getSerializer(MaskingPattern.class);
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
    if (input == null || input.length == 0) {
      error(remarks, transformMeta, "MaskFields.Check.NoInput");
    }

    Set<String> seen = new HashSet<>();
    if (fields != null) {
      for (MaskField field : fields) {
        if (field == null || StringUtils.isEmpty(field.getFieldName())) {
          continue;
        }
        String fieldName = field.getFieldName();
        if (!seen.add(fieldName)) {
          error(remarks, transformMeta, "MaskFields.Check.DuplicateField", fieldName);
        }
        if (prev != null && prev.indexOfValue(fieldName) < 0) {
          error(remarks, transformMeta, "MaskFields.Check.MissingField", fieldName);
        }
        if (StringUtils.isEmpty(field.getPatternName())) {
          continue;
        }
        MaskingPattern pattern = loadQuietly(metadataProvider, field.getPatternName());
        if (pattern == null) {
          error(remarks, transformMeta, "MaskFields.Check.MissingPattern", field.getPatternName());
          continue;
        }
        if (prev != null) {
          IValueMeta valueMeta = prev.searchValueMeta(fieldName);
          if (valueMeta != null) {
            String problem = MaskingRules.incompatibility(valueMeta, pattern);
            if (problem != null) {
              error(
                  remarks,
                  transformMeta,
                  "MaskFields.Check.Incompatible",
                  fieldName,
                  pattern.getName(),
                  problem);
            }
          }
        }
        checkDatabase(remarks, transformMeta, variables, metadataProvider, pattern);
      }
    }
  }

  private void checkDatabase(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      MaskingPattern pattern) {
    if (pattern.getStorage() != MaskingStorage.DATABASE
        || pattern.getValueSource() == MaskingValueSource.SET_NULL
        || pattern.getValueSource() == MaskingValueSource.SET_EMPTY) {
      return;
    }
    if (StringUtils.isEmpty(pattern.getConnection())) {
      error(remarks, transformMeta, "MaskFields.Check.NoConnection", pattern.getName());
      return;
    }
    if (StringUtils.isEmpty(pattern.getTableName())) {
      error(remarks, transformMeta, "MaskFields.Check.NoTable", pattern.getName());
    }
    if (StringUtils.isEmpty(pattern.getHashSecret())) {
      warning(remarks, transformMeta, "MaskFields.Check.PlainTextKeys", pattern.getName());
    }
    int longest =
        resolved(variables, pattern.getPrefix()).length()
            + (pattern.getToken() == MaskingToken.UUID ? UUID_LENGTH : LONG_DIGITS)
            + resolved(variables, pattern.getSuffix()).length();
    if (longest > DatabaseMaskingStore.MASKED_LENGTH) {
      warning(
          remarks,
          transformMeta,
          "MaskFields.Check.MaskedTooLong",
          pattern.getName(),
          Integer.toString(longest),
          Integer.toString(DatabaseMaskingStore.MASKED_LENGTH));
    }
    if (metadataProvider == null) {
      return;
    }
    try {
      String name =
          variables == null ? pattern.getConnection() : variables.resolve(pattern.getConnection());
      DatabaseMeta database = metadataProvider.getSerializer(DatabaseMeta.class).load(name);
      if (database == null) {
        error(
            remarks, transformMeta, "MaskFields.Check.MissingConnection", name, pattern.getName());
      }
    } catch (HopException e) {
      error(
          remarks,
          transformMeta,
          "MaskFields.Check.MissingConnection",
          pattern.getConnection(),
          pattern.getName());
    }
  }

  private MaskingPattern loadQuietly(IHopMetadataProvider metadataProvider, String name) {
    try {
      return loadPattern(metadataProvider, name);
    } catch (HopException e) {
      return null;
    }
  }

  private static String resolved(IVariables variables, String value) {
    String text = variables == null ? value : variables.resolve(value);
    return text == null ? "" : text;
  }

  private void warning(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... args) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, key, (Object[]) args),
            transformMeta));
  }

  private void error(
      List<ICheckResult> remarks, TransformMeta transformMeta, String key, String... args) {
    remarks.add(
        new CheckResult(
            ICheckResult.TYPE_RESULT_ERROR,
            BaseMessages.getString(PKG, key, (Object[]) args),
            transformMeta));
  }
}
