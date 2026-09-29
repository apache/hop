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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SplitFieldToRowsMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void loadSaveTest() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "splitField",
            "delimiter",
            "enclosure",
            "newFieldname",
            "includeRowNumber",
            "rowNumberField",
            "resetRowNumber",
            "delimiterRegex",
            "excludeSplitField");

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("includeRowNumber", "isIncludeRowNumber");
    getterMap.put("resetRowNumber", "isResetRowNumber");
    getterMap.put("delimiterRegex", "isIsDelimiterRegex");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("delimiterRegex", "setIsDelimiterRegex");

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();

    LoadSaveTester loadSaveTester =
        new LoadSaveTester(
            SplitFieldToRowsMeta.class,
            attributes,
            getterMap,
            setterMap,
            fieldLoadSaveValidatorAttributeMap,
            new HashMap<>());

    loadSaveTester.testSerialization();
  }

  @Test
  void getFieldsKeepsSplitFieldByDefault() throws Exception {
    SplitFieldToRowsMeta meta = meta("licenses_string", "licenses");

    IRowMeta row = inputRow();
    meta.getFields(row, "split", null, null, new Variables(), null);

    assertEquals(List.of("id", "licenses_string", "name", "licenses"), names(row));
  }

  @Test
  void getFieldsRemovesSplitFieldWhenExcluded() throws Exception {
    SplitFieldToRowsMeta meta = meta("licenses_string", "licenses");
    meta.setExcludeSplitField(true);

    IRowMeta row = inputRow();
    meta.getFields(row, "split", null, null, new Variables(), null);

    assertEquals(List.of("id", "name", "licenses"), names(row));
    assertTrue(row.searchValueMeta("licenses").isString());
  }

  @Test
  void getFieldsRemovesSplitFieldResolvedFromVariable() throws Exception {
    SplitFieldToRowsMeta meta = meta("${FIELD}", "licenses");
    meta.setExcludeSplitField(true);
    Variables variables = new Variables();
    variables.setVariable("FIELD", "licenses_string");

    IRowMeta row = inputRow();
    meta.getFields(row, "split", null, null, variables, null);

    assertEquals(List.of("id", "name", "licenses"), names(row));
  }

  @Test
  void getFieldsLeavesOtherFieldsWhenSplitFieldIsMissing() throws Exception {
    SplitFieldToRowsMeta meta = meta("missing", "licenses");
    meta.setExcludeSplitField(true);

    IRowMeta row = inputRow();
    meta.getFields(row, "split", null, null, new Variables(), null);

    assertEquals(List.of("id", "licenses_string", "name", "licenses"), names(row));
  }

  @Test
  void getFieldsAppendsRowNumberAfterRemovingSplitField() throws Exception {
    SplitFieldToRowsMeta meta = meta("licenses_string", "licenses");
    meta.setExcludeSplitField(true);
    meta.setIncludeRowNumber(true);
    meta.setRowNumberField("${NR}");
    Variables variables = new Variables();
    variables.setVariable("NR", "nr");

    IRowMeta row = inputRow();
    meta.getFields(row, "split", null, null, variables, null);

    assertEquals(List.of("id", "name", "licenses", "nr"), names(row));
    assertTrue(row.searchValueMeta("nr").isInteger());
  }

  private static SplitFieldToRowsMeta meta(String splitField, String newFieldname) {
    SplitFieldToRowsMeta meta = new SplitFieldToRowsMeta();
    meta.setSplitField(splitField);
    meta.setNewFieldname(newFieldname);
    return meta;
  }

  private static IRowMeta inputRow() {
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("id"));
    row.addValueMeta(new ValueMetaString("licenses_string"));
    row.addValueMeta(new ValueMetaString("name"));
    return row;
  }

  private static List<String> names(IRowMeta row) {
    return List.of(row.getFieldNames());
  }
}
