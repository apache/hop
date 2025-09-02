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

package org.apache.hop.pipeline.transforms.selectvalues;

import static org.apache.hop.pipeline.transforms.selectvalues.SelectValueMetaTestFactory.getSelectFields;
import static org.apache.hop.pipeline.transforms.selectvalues.SelectValueMetaTestFactory.getSelectFieldsWithRename;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SelectValuesMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String FIRST_NAME = "FIRST_FIELD";

  private static final String SECOND_NAME = "SECOND_FIELD";

  private static final String FIRST_RENAME = "FIRST_FIELD_RENAMED";

  private static final String SECOND_RENAME = "SECOND_FIELD_RENAMED";

  private SelectValuesMeta selectValuesMeta;

  @BeforeEach
  void before() {
    selectValuesMeta = new SelectValuesMeta();
  }

  @Disabled("This test needs to be reviewed")
  @Test
  void loadSaveTest() throws HopException {
    List<String> attributes = Arrays.asList("selectOption", "deleteName");

    SelectField selectField = new SelectField();
    selectField.setName("TEST_NAME");
    selectField.setRename("TEST_RENAME");
    selectField.setLength(2);
    selectField.setPrecision(2);

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap = new HashMap<>();
    fieldLoadSaveValidatorTypeMap.put(
        SelectField[].class.getCanonicalName(),
        new ArrayLoadSaveValidator<>(new SelectFieldLoadSaveValidator(selectField), 2));

    LoadSaveTester tester =
        new LoadSaveTester(
            SelectValuesMeta.class,
            attributes,
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            fieldLoadSaveValidatorTypeMap);

    tester.testSerialization();
  }

  @Test
  void setSelectName() {
    List<SelectField> fields = getSelectFields(FIRST_NAME, SECOND_NAME);

    selectValuesMeta.getSelectOption().setSelectFields(fields);

    assertEquals(fields, selectValuesMeta.getSelectOption().getSelectFields());
  }

  @Test
  void setSelectName_getOtherFields() {

    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME, SECOND_NAME));

    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .allMatch(field -> field.getRename() == null)
        .allMatch(field -> field.getLength() == SelectValuesMeta.UNDEFINED)
        .allMatch(field -> field.getPrecision() == SelectValuesMeta.UNDEFINED);
  }

  @Test
  void setSelectName_smallerThanPrevious() {
    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME, SECOND_NAME));
    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME));
    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .extracting(SelectField::getName)
        .containsExactly(FIRST_NAME);
  }

  @Test
  void getSelectName() {
    assertThat(selectValuesMeta.getSelectName()).isEmpty();
  }

  @Test
  void setSelectRename() {
    selectValuesMeta
        .getSelectOption()
        .setSelectFields(
            getSelectFieldsWithRename(
                List.of(FIRST_NAME, SECOND_NAME), List.of(FIRST_RENAME, SECOND_RENAME)));
    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .extracting(SelectField::getRename)
        .containsExactly(FIRST_RENAME, SECOND_RENAME);
  }

  @Test
  void setSelectRename_getOtherFields() {
    selectValuesMeta
        .getSelectOption()
        .setSelectFields(
            getSelectFieldsWithRename(
                List.of(FIRST_NAME, SECOND_NAME), List.of(FIRST_RENAME, SECOND_RENAME)));

    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .allMatch(field -> field.getName() != null)
        .allMatch(field -> List.of(FIRST_NAME, SECOND_NAME).contains(field.getName()))
        .allMatch(field -> field.getLength() == SelectValuesMeta.UNDEFINED)
        .allMatch(field -> field.getPrecision() == SelectValuesMeta.UNDEFINED);
  }

  @Test
  void setSelectRename_smallerThanPrevious() {
    selectValuesMeta
        .getSelectOption()
        .setSelectFields(
            getSelectFieldsWithRename(
                List.of(FIRST_NAME, SECOND_NAME), List.of(FIRST_RENAME, SECOND_RENAME)));
    selectValuesMeta
        .getSelectOption()
        .setSelectFields(getSelectFieldsWithRename(List.of(FIRST_NAME), List.of(FIRST_RENAME)));

    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .extracting(SelectField::getRename)
        .containsExactly(FIRST_RENAME);
  }

  @Test
  void getSelectRename() {
    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .allMatch(field -> field.getRename().isEmpty());
  }

  @Test
  void setSelectLength() {
    List<SelectField> selectFields = getSelectFields(FIRST_NAME, SECOND_NAME);
    selectFields.get(0).setLength(1);
    selectFields.get(1).setLength(2);

    assertThat(selectFields).extracting(SelectField::getLength).containsExactly(1, 2);
  }

  @Test
  void setSelectLength_getOtherFields() {
    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME, SECOND_NAME));
    selectValuesMeta.getSelectOption().getSelectFields().get(0).setLength(1);
    selectValuesMeta.getSelectOption().getSelectFields().get(1).setLength(2);
    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .allMatch(field -> field.getName() != null)
        .allMatch(field -> List.of(FIRST_NAME, SECOND_NAME).contains(field.getName()))
        .allMatch(field -> field.getRename() == null)
        .allMatch(field -> field.getPrecision() == SelectValuesMeta.UNDEFINED);
  }

  @Test
  void setSelectLength_smallerThanPrevious() {
    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME, SECOND_NAME));
    selectValuesMeta.getSelectOption().getSelectFields().get(0).setLength(1);
    selectValuesMeta.getSelectOption().getSelectFields().get(1).setLength(2);
    selectValuesMeta.getSelectOption().getSelectFields().remove(1);

    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .extracting(SelectField::getLength)
        .containsExactly(1);
  }

  @Test
  void setSelectPrecision() {
    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME, SECOND_NAME));
    selectValuesMeta.getSelectOption().getSelectFields().get(0).setPrecision(1);
    selectValuesMeta.getSelectOption().getSelectFields().get(1).setPrecision(2);

    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .extracting(SelectField::getPrecision)
        .containsExactly(1, 2);
  }

  @Test
  void setSelectPrecision_smallerThanPrevious() {

    selectValuesMeta.getSelectOption().setSelectFields(getSelectFields(FIRST_NAME, SECOND_NAME));
    selectValuesMeta.getSelectOption().getSelectFields().get(0).setPrecision(1);
    selectValuesMeta.getSelectOption().getSelectFields().get(1).setPrecision(2);
    selectValuesMeta.getSelectOption().getSelectFields().remove(0);

    assertThat(selectValuesMeta.getSelectOption().getSelectFields())
        .extracting(SelectField::getPrecision)
        .containsExactly(2);
  }

  public static class SelectFieldLoadSaveValidator implements IFieldLoadSaveValidator<SelectField> {

    private final SelectField defaultValue;

    public SelectFieldLoadSaveValidator(SelectField defaultValue) {
      this.defaultValue = defaultValue;
    }

    @Override
    public SelectField getTestObject() {
      return defaultValue;
    }

    @Override
    public boolean validateTestObject(SelectField testObject, Object actual) {
      return EqualsBuilder.reflectionEquals(testObject, actual);
    }
  }

  @Test
  void testNewSerialization() throws Exception {
    SelectValuesMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/select-values-transform.xml", SelectValuesMeta.class);

    assertEquals(4, meta.getSelectOption().getSelectFields().size());
    assertEquals(1, meta.getSelectOption().getDeleteName().size());
    assertEquals(1, meta.getSelectOption().getMeta().size());
  }

  @Test
  void testClone() throws Exception {
    SelectValuesMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/select-values-transform.xml", SelectValuesMeta.class);

    SelectValuesMeta clone = (SelectValuesMeta) meta.clone();
  }
}
