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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FormulaMetaTest {

  private FormulaMeta formulaMeta;

  @BeforeEach
  void setUp() {
    formulaMeta = new FormulaMeta();
  }

  @Test
  void testDefaultConstructor() {
    assertNotNull(formulaMeta.getFormulas());
    assertTrue(formulaMeta.getFormulas().isEmpty());
  }

  @Test
  void testCopyConstructor() {
    List<FormulaMetaFunction> formulas = new ArrayList<>();
    formulas.add(new FormulaMetaFunction("test", "1+1", 1, 10, 2, "", false));
    formulaMeta.setFormulas(formulas);

    FormulaMeta copy = new FormulaMeta(formulaMeta);
    assertEquals(formulaMeta.getFormulas(), copy.getFormulas());
  }

  @Test
  void testGetFields_ReplaceField() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field_to_replace"));

    FormulaMetaFunction formula =
        new FormulaMetaFunction(
            "", "UPPER([field_to_replace])", 2, 20, 0, "field_to_replace", false);
    List<FormulaMetaFunction> formulas = new ArrayList<>();
    formulas.add(formula);
    formulaMeta.setFormulas(formulas);

    formulaMeta.getFields(rowMeta, "transform", null, null, new Variables(), null);

    assertEquals(1, rowMeta.size());
    assertEquals("field_to_replace", rowMeta.getValueMeta(0).getName());
    assertEquals(2, rowMeta.getValueMeta(0).getType());
  }

  @Test
  void testGetFields_ReplaceFieldNotFound() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("existing_field"));

    FormulaMetaFunction formula =
        new FormulaMetaFunction("", "1+1", 1, 10, 2, "non_existing_field", false);
    List<FormulaMetaFunction> formulas = new ArrayList<>();
    formulas.add(formula);
    formulaMeta.setFormulas(formulas);

    assertThrows(
        HopTransformException.class,
        () -> {
          formulaMeta.getFields(rowMeta, "transform", null, null, new Variables(), null);
        });
  }

  @Test
  void testGetFields_EmptyFieldName() throws Exception {
    IRowMeta rowMeta = new RowMeta();

    FormulaMetaFunction formula = new FormulaMetaFunction("", "1+1", 1, 10, 2, "", false);
    List<FormulaMetaFunction> formulas = new ArrayList<>();
    formulas.add(formula);
    formulaMeta.setFormulas(formulas);

    int originalSize = rowMeta.size();
    formulaMeta.getFields(rowMeta, "transform", null, null, new Variables(), null);

    // No new field should be added if fieldName is empty and not replacing
    assertEquals(originalSize, rowMeta.size());
  }

  @Test
  void testFormulasGetterSetter() {
    List<FormulaMetaFunction> formulas = new ArrayList<>();
    formulas.add(new FormulaMetaFunction("test", "1+1", 1, 10, 2, "", false));

    formulaMeta.setFormulas(formulas);
    assertEquals(formulas, formulaMeta.getFormulas());
    assertEquals(1, formulaMeta.getFormulas().size());
  }
}
