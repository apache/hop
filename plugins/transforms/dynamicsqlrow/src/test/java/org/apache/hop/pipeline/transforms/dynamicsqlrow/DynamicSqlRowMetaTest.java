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
package org.apache.hop.pipeline.transforms.dynamicsqlrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link DynamicSqlRowMeta} */
class DynamicSqlRowMetaTest {

  LoadSaveTester<DynamicSqlRowMeta> loadSaveTester;
  Class<DynamicSqlRowMeta> testMetaClass = DynamicSqlRowMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @BeforeEach
  void setUpLoadSave() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "sql",
            "sqlFieldName",
            "rowLimit",
            "outerJoin",
            "replaceVariables",
            "queryOnlyOnChange",
            "connection");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<>(
            testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap);
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  void testSetDefault() {
    // valid default value
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    meta.setDefault();

    assertNull(meta.getConnection());
    assertNull(meta.getDatabaseMeta());
    assertEquals(0, meta.getRowLimit());
    assertEquals("", meta.getSql());
    assertFalse(meta.isOuterJoin());
    assertFalse(meta.isReplaceVariables());
    assertNull(meta.getSqlFieldName());
    assertFalse(meta.isQueryOnlyOnChange());
  }

  @Test
  void testGettersAndSetters() {
    // valid property
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();

    meta.setConnection("conn");
    assertEquals("conn", meta.getConnection());

    meta.setSql("SELECT 1");
    assertEquals("SELECT 1", meta.getSql());

    meta.setSqlFieldName("sql_field");
    assertEquals("sql_field", meta.getSqlFieldName());

    meta.setRowLimit(10);
    assertEquals(10, meta.getRowLimit());

    meta.setOuterJoin(true);
    assertTrue(meta.isOuterJoin());

    meta.setReplaceVariables(true);
    assertTrue(meta.isReplaceVariables());

    meta.setQueryOnlyOnChange(true);
    assertTrue(meta.isQueryOnlyOnChange());
  }

  @Test
  void testClone() {
    // valid clone
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    meta.setConnection("conn");
    meta.setSql("SELECT id FROM t");
    meta.setSqlFieldName("sql_field");
    meta.setRowLimit(5);
    meta.setOuterJoin(true);
    meta.setReplaceVariables(true);
    meta.setQueryOnlyOnChange(true);

    DynamicSqlRowMeta cloned = (DynamicSqlRowMeta) meta.clone();

    assertNotNull(cloned);
    assertEquals(meta.getConnection(), cloned.getConnection());
    assertEquals(meta.getSql(), cloned.getSql());
    assertEquals(meta.getSqlFieldName(), cloned.getSqlFieldName());
    assertEquals(meta.getRowLimit(), cloned.getRowLimit());
    assertEquals(meta.isOuterJoin(), cloned.isOuterJoin());
    assertEquals(meta.isReplaceVariables(), cloned.isReplaceVariables());
    assertEquals(meta.isQueryOnlyOnChange(), cloned.isQueryOnlyOnChange());
  }

  @Test
  void testCopyConstructor() {
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    meta.setConnection("conn");
    meta.setSql("SELECT 1");
    meta.setSqlFieldName("sql_field");
    meta.setRowLimit(3);
    meta.setOuterJoin(true);
    meta.setReplaceVariables(false);
    meta.setQueryOnlyOnChange(true);

    DynamicSqlRowMeta copy = new DynamicSqlRowMeta(meta);

    assertEquals(meta.getConnection(), copy.getConnection());
    assertEquals(meta.getSql(), copy.getSql());
    assertEquals(meta.getSqlFieldName(), copy.getSqlFieldName());
    assertEquals(meta.getRowLimit(), copy.getRowLimit());
    assertEquals(meta.isOuterJoin(), copy.isOuterJoin());
    assertEquals(meta.isReplaceVariables(), copy.isReplaceVariables());
    assertEquals(meta.isQueryOnlyOnChange(), copy.isQueryOnlyOnChange());
  }

  @Test
  void supportsErrorHandlingReturnsTrue() {
    assertTrue(new DynamicSqlRowMeta().supportsErrorHandling());
  }

  @Test
  void checkWithoutInputReportsError() {
    // valid check
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected error when no input transforms are connected");
  }

  @Test
  void checkWithInputAndMissingSqlFieldNameReportsError() {
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected error when SQL field name is missing");
  }

  @Test
  void checkWithUnknownSqlFieldReportsError() {
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    meta.setSqlFieldName("sql");
    List<ICheckResult> remarks = new ArrayList<>();

    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected error when SQL field is not present in previous transform output");
  }

  @Test
  void checkWithValidSqlFieldReportsOk() {
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    meta.setSqlFieldName("sql");

    RowMeta prev = new RowMeta();
    IValueMeta sqlField = new ValueMetaString("sql");
    sqlField.setOrigin("input");
    prev.addValueMeta(sqlField);

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK),
        "Expected OK result when SQL field exists in previous transform output");
  }

  @Test
  void checkWithoutDatabaseMetaReportsInvalidConnection() {
    DynamicSqlRowMeta meta = new DynamicSqlRowMeta();
    meta.setSqlFieldName("sql_field");

    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("sql_field"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        prev,
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "Expected error when database connection metadata is missing");
  }
}
