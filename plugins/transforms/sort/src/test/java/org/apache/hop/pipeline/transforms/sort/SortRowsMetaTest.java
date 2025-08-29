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

package org.apache.hop.pipeline.transforms.sort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

public class SortRowsMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  /**
   * @throws HopException
   */
  @Test
  public void testRoundTrips() throws Exception {
    List<String> attributes =
        Arrays.asList(
            "Directory",
            "Prefix",
            "SortSize",
            "FreeMemoryLimit",
            "CompressFiles",
            "CompressFilesVariable",
            "OnlyPassingUniqueRows",
            "SortFields");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();

    IFieldLoadSaveValidator<String> stringFieldLoadSaveValidator = new StringLoadSaveValidator();
    IFieldLoadSaveValidator<Boolean> booleanFieldLoadSaveValidator = new BooleanLoadSaveValidator();

    fieldLoadSaveValidatorAttributeMap.put("Directory", stringFieldLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("Prefix", stringFieldLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("SortSize", stringFieldLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("FreeMemoryLimit", stringFieldLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("CompressFiles", booleanFieldLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("CompressFilesVariable", stringFieldLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("OnlyPassingUniqueRows", booleanFieldLoadSaveValidator);

    LoadSaveTester<SortRowsMeta> loadSaveTester =
        new LoadSaveTester<>(
            SortRowsMeta.class,
            attributes,
            getterMap,
            setterMap,
            fieldLoadSaveValidatorAttributeMap,
            new HashMap<>());

    loadSaveTester
        .getFieldLoadSaveValidatorFactory()
        .registerValidator(
            SortRowsMeta.class.getDeclaredField("sortFields").getGenericType().toString(),
            new ListLoadSaveValidator<>(new SortRowsFieldLoadSaveValidator()));
    loadSaveTester.testSerialization();
  }

  private final class SortRowsFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<SortRowsField> {

    @Override
    public SortRowsField getTestObject() {
      return new SortRowsField(UUID.randomUUID().toString(), true, true, true, 0, true);
    }

    @Override
    public boolean validateTestObject(SortRowsField testObject, Object actual) throws HopException {
      if (!(actual instanceof SortRowsField)) {
        return false;
      }
      SortRowsField actualObject = (SortRowsField) actual;
      return testObject.getFieldName().equals(actualObject.getFieldName())
          && testObject.isAscending() == actualObject.isAscending()
          && testObject.isCaseSensitive() == actualObject.isCaseSensitive()
          && testObject.isCollatorEnabled() == actualObject.isCollatorEnabled()
          && testObject.getCollatorStrength() == actualObject.getCollatorStrength()
          && testObject.isPreSortedField() == actualObject.isPreSortedField();
    }
  }

  @Test
  public void testGetDefaultStrength() {
    SortRowsMeta srm = new SortRowsMeta();
    int usStrength = srm.getDefaultCollationStrength(Locale.US);
    assertEquals(Collator.TERTIARY, usStrength);
    assertEquals(Collator.IDENTICAL, srm.getDefaultCollationStrength(null));
  }

  @Test
  public void testPDI16559() throws Exception {
    SortRowsMeta sortRowsReal = new SortRowsMeta();
    SortRowsMeta sortRows = Mockito.spy(sortRowsReal);
    sortRows.setDirectory("/tmp");
    List<SortRowsField> sortRowsFields = new ArrayList<>();
    sortRowsFields.add(new SortRowsField("field1", false, true, false, 2, true));
    sortRowsFields.add(new SortRowsField("field2", true, false, false, 1, true));
    sortRowsFields.add(new SortRowsField("field3", false, true, true, 3, false));
    sortRowsFields.add(new SortRowsField("field4", true, false, false, 0, false));
    sortRowsFields.add(new SortRowsField("field5", true, true, false, 0, false));
    sortRows.setSortFields(sortRowsFields);

    // check the field names
    for (int i = 0; i < sortRows.getSortFields().size(); i++) {
      SortRowsField sortRowsField = sortRows.getSortFields().get(i);
      assertEquals("field" + (i + 1), sortRowsField.getFieldName());
    }

    // check the properties for SortRowField 3.
    assertFalse(sortRowsFields.get(2).isAscending());
    assertTrue(sortRowsFields.get(2).isCaseSensitive());
    assertTrue(sortRowsFields.get(2).isCollatorEnabled());
    assertEquals(3, sortRowsFields.get(2).getCollatorStrength());
    assertFalse(sortRowsFields.get(2).isPreSortedField());
  }
}
