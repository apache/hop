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

package org.apache.hop.pipeline.transforms.formula;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "Formula",
    image = "formula.svg",
    name = "i18n::Formula.name",
    description = "i18n::Formula.description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Scripting",
    keywords = "i18n::Formula.keywords",
    documentationUrl = "/pipeline/transforms/formula.html")
public class FormulaMeta extends BaseTransformMeta<Formula, FormulaData> {

  /** The formula calculations to be performed */
  @HopMetadataProperty(
      groupKey = "formulas",
      key = "formula",
      injectionGroupDescription = "FormulaMeta.Injection.Formulas",
      injectionKeyDescription = "FormulaMeta.Injection.Formula")
  private List<FormulaMetaFunction> formulas;

  public FormulaMeta() {
    super();
    formulas = new ArrayList<>();
  }

  public FormulaMeta(FormulaMeta m) {
    this.formulas = m.formulas;
  }

  public void setFormulas(List<FormulaMetaFunction> formulas) {
    this.formulas = formulas;
  }

  public List<FormulaMetaFunction> getFormulas() {
    return formulas;
  }

  @Override
  public Object clone() {
    return new FormulaMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (FormulaMetaFunction formula : formulas) {
      if (Utils.isEmpty(formula.getReplaceField())) {
        // Not replacing a field.
        if (!Utils.isEmpty(formula.getFieldName())) {
          // It's a new field!

          try {
            IValueMeta v =
                ValueMetaFactory.createValueMeta(formula.getFieldName(), formula.getValueType());
            v.setLength(formula.getValueLength(), formula.getValuePrecision());
            v.setOrigin(name);
            row.addValueMeta(v);
          } catch (Exception e) {
            throw new HopTransformException(e);
          }
        }
      } else {
        // Replacing a field
        int index = row.indexOfValue(formula.getReplaceField());
        if (index < 0) {
          throw new HopTransformException(
              "Unknown field specified to replace with a formula result: ["
                  + formula.getReplaceField()
                  + "]");
        }
        // Change the data type etc.
        //
        IValueMeta v = row.getValueMeta(index).clone();
        v.setLength(formula.getValueLength(), formula.getValuePrecision());
        v.setOrigin(name);
        row.setValueMeta(index, v); // replace it
      }
    }
  }
}
