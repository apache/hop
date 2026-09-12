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
package org.apache.hop.pipeline.transforms.databasejoin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link DatabaseJoinMeta} */
class DatabaseJoinMetaTest implements IInitializer<DatabaseJoinMeta> {
  LoadSaveTester<DatabaseJoinMeta> loadSaveTester;
  Class<DatabaseJoinMeta> testMetaClass = DatabaseJoinMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @BeforeEach
  void setUpLoadSave() throws Exception {
    List<String> attributes =
        Arrays.asList(
            "sql",
            "sqlFromFile",
            "rowLimit",
            "outerJoin",
            "replaceVariables",
            "connection",
            "cached",
            "cacheSize",
            "parameters");

    Map<String, String> getterMap = new HashMap<>();

    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "parameters", new ListLoadSaveValidator<>(new ParameterFieldLoadSaveValidator(), 5));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<>(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(DatabaseJoinMeta someMeta) {
    // Do nothing
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  void getEffectiveSqlUsesInlineWhenNoFile() throws Exception {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setSql("SELECT 1");
    Assertions.assertEquals("SELECT 1", meta.getEffectiveSql(new Variables()));
  }

  @Test
  void getEffectiveSqlLoadsFromFile() throws Exception {
    Path file = Files.createTempFile("databasejoin-", ".sql");
    try {
      String sql = "SELECT * FROM lookup WHERE id = ?";
      Files.writeString(file, sql);
      DatabaseJoinMeta meta = new DatabaseJoinMeta();
      meta.setSql("SELECT 1");
      meta.setSqlFromFile(file.toAbsolutePath().toString());
      Assertions.assertEquals(sql, meta.getEffectiveSql(new Variables()));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  void getEffectiveSqlResolvesVariablesInPath() throws Exception {
    Path file = Files.createTempFile("databasejoin-", ".sql");
    try {
      Files.writeString(file, "SELECT 2");
      DatabaseJoinMeta meta = new DatabaseJoinMeta();
      meta.setSqlFromFile("${SQL_FILE}");
      Variables variables = new Variables();
      variables.setVariable("SQL_FILE", file.toAbsolutePath().toString());
      Assertions.assertEquals("SELECT 2", meta.getEffectiveSql(variables));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  void getEffectiveSqlMissingFileThrows() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setSqlFromFile("/this/path/does/not/exist-databasejoin.sql");
    Assertions.assertThrows(HopException.class, () -> meta.getEffectiveSql(new Variables()));
  }

  @Test
  void getEffectiveSqlEmptyFilePathUsesInlineSql() throws Exception {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setSql("SELECT 1");
    meta.setSqlFromFile("");
    Assertions.assertEquals("SELECT 1", meta.getEffectiveSql(new Variables()));
  }

  @Test
  void setDefaultResetsLookupOptions() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setSql("SELECT 1");
    meta.setRowLimit(10);
    meta.setOuterJoin(true);
    meta.setReplaceVariables(true);
    ParameterField field = new ParameterField();
    field.setName("id");
    meta.getParameters().add(field);

    meta.setDefault();

    Assertions.assertEquals("", meta.getSql());
    Assertions.assertEquals(0, meta.getRowLimit());
    Assertions.assertFalse(meta.isOuterJoin());
    Assertions.assertFalse(meta.isReplaceVariables());
    Assertions.assertTrue(meta.getParameters().isEmpty());
  }

  @Test
  void supportsErrorHandling() {
    Assertions.assertTrue(new DatabaseJoinMeta().supportsErrorHandling());
  }

  @Test
  void getFieldsDoesNothingWhenConnectionIsMissing() throws Exception {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("id"));
    meta.getFields(row, "join", null, null, new Variables(), null);
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals("id", row.getValueMeta(0).getName());
  }

  @Test
  void getParameterRowSelectsMatchingIncomingFieldsInOrder() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    ParameterField id = new ParameterField();
    id.setName("id");
    ParameterField missing = new ParameterField();
    missing.setName("missing");
    ParameterField name = new ParameterField();
    name.setName("name");
    meta.setParameters(List.of(id, missing, name));

    IRowMeta incoming = new RowMeta();
    incoming.addValueMeta(new ValueMetaString("name"));
    incoming.addValueMeta(new ValueMetaInteger("id"));
    incoming.addValueMeta(new ValueMetaString("extra"));

    IRowMeta param = meta.getParameterRow(incoming);
    Assertions.assertEquals(2, param.size());
    Assertions.assertEquals("id", param.getValueMeta(0).getName());
    Assertions.assertEquals("name", param.getValueMeta(1).getName());
  }

  @Test
  void getParameterRowHandlesNullIncoming() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    ParameterField field = new ParameterField();
    field.setName("id");
    meta.getParameters().add(field);
    IRowMeta param = meta.getParameterRow(null);
    Assertions.assertNotNull(param);
    Assertions.assertTrue(param.isEmpty());
  }

  @Test
  void parseSqlParameterSpecSupportsMixedNamedAndPositionalPlaceholders() {
    String sql =
        "SELECT order_id, total FROM orders WHERE customer_id = ?{customer_id} AND status = ?";

    DatabaseJoinMeta.SqlParameterSpec spec = DatabaseJoinMeta.parseSqlParameterSpec(sql);

    Assertions.assertEquals(
        "SELECT order_id, total FROM orders WHERE customer_id = ? AND status = ?",
        spec.getPreparedSql());
    Assertions.assertEquals(2, spec.getParameterCount());
    Assertions.assertEquals(1, spec.getPositionalParameterCount());
    Assertions.assertEquals("customer_id", spec.getParameterReferences().get(0));
    Assertions.assertNull(spec.getParameterReferences().get(1));
  }

  @Test
  void parseSqlParameterSpecForSqlServerExecUsesJdbcPositionalMarkers() {
    String sql = "exec usp_WOAvgCycleTimeByOp @OrderNumber = ?{order}, @RouteName = ?{route};";

    DatabaseJoinMeta.SqlParameterSpec spec = DatabaseJoinMeta.parseSqlParameterSpec(sql);

    Assertions.assertEquals(
        "exec usp_WOAvgCycleTimeByOp @OrderNumber = ?, @RouteName = ?;", spec.getPreparedSql());
    Assertions.assertEquals(List.of("order", "route"), spec.getParameterReferences());
    Assertions.assertEquals(0, spec.getPositionalParameterCount());
  }

  @Test
  void createQueryParameterRowMetaResolvesNamedAndPositionalOrder() throws Exception {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    ParameterField positional = new ParameterField();
    positional.setName("status");
    positional.setType(IValueMeta.TYPE_STRING);
    meta.setParameters(List.of(positional));

    IRowMeta source = new RowMeta();
    source.addValueMeta(new ValueMetaInteger("customer_id"));
    source.addValueMeta(new ValueMetaString("status"));

    DatabaseJoinMeta.SqlParameterSpec spec =
        DatabaseJoinMeta.parseSqlParameterSpec(
            "SELECT 1 FROM dual WHERE customer_id = ?{customer_id} AND status = ?");

    IRowMeta queryParamMeta = meta.createQueryParameterRowMeta(source, spec);
    Assertions.assertEquals(2, queryParamMeta.size());
    Assertions.assertEquals("customer_id", queryParamMeta.getValueMeta(0).getName());
    Assertions.assertEquals("status", queryParamMeta.getValueMeta(1).getName());
  }

  @Test
  void createMetadataLookupParameterRowDataBuildsTypedDefaults() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    ParameterField intField = new ParameterField();
    intField.setName("order");
    intField.setType(IValueMeta.TYPE_INTEGER);
    ParameterField boolField = new ParameterField();
    boolField.setName("active");
    boolField.setType(IValueMeta.TYPE_BOOLEAN);
    ParameterField textField = new ParameterField();
    textField.setName("route");
    textField.setType(IValueMeta.TYPE_STRING);
    meta.setParameters(List.of(intField, boolField, textField));

    DatabaseJoinMeta.SqlParameterSpec spec =
        DatabaseJoinMeta.parseSqlParameterSpec(
            "exec usp_WOAvgCycleTimeByOp @OrderNumber = ?{order}, @Active = ?{active}, @RouteName = ?{route}");
    IRowMeta queryParamMeta = meta.createMetadataLookupParameterRowMeta(spec);
    Object[] rowData = meta.createMetadataLookupParameterRowData(queryParamMeta);

    Assertions.assertEquals(3, rowData.length);
    Assertions.assertEquals(Long.valueOf(0L), rowData[0]);
    Assertions.assertEquals(Boolean.FALSE, rowData[1]);
    Assertions.assertEquals("metadata", rowData[2]);
  }

  public class ParameterFieldLoadSaveValidator implements IFieldLoadSaveValidator<ParameterField> {

    @Override
    public ParameterField getTestObject() {
      ParameterField field = new ParameterField();
      field.setName(UUID.randomUUID().toString());
      field.setType(IValueMeta.TYPE_STRING);

      return field;
    }

    @Override
    public boolean validateTestObject(ParameterField testObject, Object actual) {
      if (!(actual instanceof ParameterField)) {
        return false;
      }
      ParameterField another = (ParameterField) actual;
      return new EqualsBuilder()
          .append(testObject.getName(), another.getName())
          .append(testObject.getType(), another.getType())
          .isEquals();
    }
  }
}
