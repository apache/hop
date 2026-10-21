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

package org.apache.hop.pipeline.transforms.openformula;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Metadata for the OpenFormula transform.
 *
 * <p>Plugin id {@code OpenFormula} is distinct from Hop's POI {@code Formula} transform. Kettle/PDI
 * Formula steps are imported as this id.
 */
@Getter
@Setter
@GuiPlugin
@Transform(
    id = "OpenFormula",
    image = "openformula.svg",
    name = "i18n::OpenFormula.Name",
    description = "i18n::OpenFormula.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::OpenFormula.keywords",
    documentationUrl = "/pipeline/transforms/openformula.html")
public class OpenFormulaMeta extends BaseTransformMeta<OpenFormula, OpenFormulaData> {

  private static final Class<?> PKG = OpenFormulaMeta.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "OpenFormulaDialog";

  /**
   * Syntax note shown above the formula grid. The grid itself is an extra group in the same box,
   * because the row stores numeric type, length, and precision that a metadata table cannot edit.
   */
  @GuiWidgetElement(
      id = "syntax",
      order = "0100",
      type = GuiElementType.LINK,
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::OpenFormulaDialog.Group.Formulas",
      groupOrder = "0100",
      label = "i18n::OpenFormulaDialog.Syntax.Label",
      toolTip = "i18n::OpenFormulaDialog.Syntax.Tooltip")
  public void syntaxHint() {
    // The label is the whole widget. Clicking it has no further action.
  }

  @HopMetadataProperty(
      groupKey = "formulas",
      key = "formula",
      injectionGroupKey = "FORMULAS",
      injectionGroupDescription = "OpenFormulaMeta.Injection.Formulas")
  private List<OpenFormulaMetaFunction> formulas;

  public OpenFormulaMeta() {
    super();
    formulas = new ArrayList<>();
  }

  public OpenFormulaMeta(OpenFormulaMeta other) {
    this();
    for (OpenFormulaMetaFunction formula : other.formulas) {
      this.formulas.add(new OpenFormulaMetaFunction(formula));
    }
  }

  @Override
  public Object clone() {
    OpenFormulaMeta copy = (OpenFormulaMeta) super.clone();
    List<OpenFormulaMetaFunction> cloned = new ArrayList<>();
    if (formulas != null) {
      for (OpenFormulaMetaFunction formula : formulas) {
        cloned.add(new OpenFormulaMetaFunction(formula));
      }
    }
    copy.formulas = cloned;
    return copy;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public void setDefault() {
    formulas = new ArrayList<>();
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
    for (OpenFormulaMetaFunction fn : formulas) {
      if (Utils.isEmpty(fn.getReplaceField())) {
        if (!Utils.isEmpty(fn.getFieldName())) {
          try {
            IValueMeta v = ValueMetaFactory.createValueMeta(fn.getFieldName(), fn.getValueType());
            v.setLength(fn.getValueLength(), fn.getValuePrecision());
            v.setOrigin(name);
            row.addValueMeta(v);
          } catch (Exception e) {
            throw new HopTransformException(e);
          }
        }
      } else {
        int index = row.indexOfValue(fn.getReplaceField());
        if (index < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "OpenFormulaMeta.Exception.UnknownReplaceField", fn.getReplaceField()));
        }
        IValueMeta v = row.getValueMeta(index).clone();
        v.setLength(fn.getValueLength(), fn.getValuePrecision());
        v.setOrigin(name);
        row.setValueMeta(index, v);
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
    if (prev == null || prev.size() == 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "OpenFormulaMeta.CheckResult.ExpectedInputError"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "OpenFormulaMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "OpenFormulaMeta.CheckResult.ExpectedInputOk"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "OpenFormulaMeta.CheckResult.ExpectedInputError"),
              transformMeta));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OpenFormulaMeta that)) {
      return false;
    }
    return Objects.equals(formulas, that.formulas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(formulas);
  }
}
