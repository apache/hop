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
package org.apache.hop.pipeline.transforms.sql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ExecSqlMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<ExecSqlMeta> testMetaClass = ExecSqlMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes =
        Arrays.asList(
            "connection",
            "sql",
            "execute_each_row",
            "update_field",
            "insert_field",
            "delete_field",
            "read_field",
            "single_statement",
            "replace_variables",
            "quoteString",
            "set_params",
            "sqlFromFile",
            "arguments");

    Map<String, String> getterMap =
        new HashMap<>() {
          {
            put("connection", "getConnection");
            put("sql", "getSql");
            put("execute_each_row", "isExecutedEachInputRow");
            put("update_field", "getUpdateField");
            put("insert_field", "getInsertField");
            put("delete_field", "getDeleteField");
            put("read_field", "getReadField");
            put("single_statement", "isSingleStatement");
            put("replace_variables", "isReplaceVariables");
            put("quoteString", "isQuoteString");
            put("set_params", "isParams");
            put("sqlFromFile", "getSqlFromFile");
            put("arguments", "getArguments");
          }
        };
    Map<String, String> setterMap =
        new HashMap<>() {
          {
            put("connection", "setConnection");
            put("sql", "setSql");
            put("execute_each_row", "setExecutedEachInputRow");
            put("update_field", "setUpdateField");
            put("insert_field", "setInsertField");
            put("delete_field", "setDeleteField");
            put("read_field", "setReadField");
            put("single_statement", "setSingleStatement");
            put("replace_variables", "setReplaceVariables");
            put("quoteString", "setQuoteString");
            put("set_params", "setParams");
            put("sqlFromFile", "setSqlFromFile");
            put("arguments", "setArguments");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(ExecSqlArgumentItem.class),
        new ObjectValidator<>(
            validatorFactory,
            ExecSqlArgumentItem.class,
            Arrays.asList("name"),
            new HashMap<>() {
              {
                put("name", "getName");
              }
            },
            new HashMap<>() {
              {
                put("name", "setName");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, ExecSqlArgumentItem.class),
        new ListLoadSaveValidator<>(new ExecSqlArgumentItemFieldLoadSaveValidator()));
  }

  public class ExecSqlArgumentItemFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<ExecSqlArgumentItem> {
    final Random rand = new Random();

    @Override
    public ExecSqlArgumentItem getTestObject() {

      return new ExecSqlArgumentItem(UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(ExecSqlArgumentItem testObject, Object actual) {
      if (!(actual instanceof ExecSqlArgumentItem)) {
        return false;
      }
      ExecSqlArgumentItem another = (ExecSqlArgumentItem) actual;
      return new EqualsBuilder().append(testObject.getName(), another.getName()).isEquals();
    }
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof ExecSqlMeta) {
      ((ExecSqlMeta) someMeta).getArguments().clear();
      ((ExecSqlMeta) someMeta)
          .getArguments()
          .addAll(
              Arrays.asList(
                  new ExecSqlArgumentItem("a"),
                  new ExecSqlArgumentItem("b"),
                  new ExecSqlArgumentItem("c"),
                  new ExecSqlArgumentItem("d"),
                  new ExecSqlArgumentItem("e")));
    }
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  void getEffectiveSqlUsesInlineWhenNoFile() throws Exception {
    ExecSqlMeta meta = new ExecSqlMeta();
    meta.setSql("SELECT 1");
    Assertions.assertEquals("SELECT 1", meta.getEffectiveSql(new Variables()));
  }

  @Test
  void getEffectiveSqlLoadsFromFile() throws Exception {
    Path file = Files.createTempFile("execsql-", ".sql");
    try {
      String sql = "insert into public.testtable (key, value) values ('k', 'v');";
      Files.writeString(file, sql);
      ExecSqlMeta meta = new ExecSqlMeta();
      meta.setSql("SELECT 1");
      meta.setSqlFromFile(file.toAbsolutePath().toString());
      Assertions.assertEquals(sql, meta.getEffectiveSql(new Variables()));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  void getEffectiveSqlResolvesVariablesInPath() throws Exception {
    Path file = Files.createTempFile("execsql-", ".sql");
    try {
      Files.writeString(file, "SELECT 2");
      ExecSqlMeta meta = new ExecSqlMeta();
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
    ExecSqlMeta meta = new ExecSqlMeta();
    meta.setSqlFromFile("/this/path/does/not-exist-execsql.sql");
    Assertions.assertThrows(HopException.class, () -> meta.getEffectiveSql(new Variables()));
  }

  @Test
  void getEffectiveSqlEmptyFilePathUsesInlineSql() throws Exception {
    ExecSqlMeta meta = new ExecSqlMeta();
    meta.setSql("SELECT 1");
    meta.setSqlFromFile("");
    Assertions.assertEquals("SELECT 1", meta.getEffectiveSql(new Variables()));
  }
}
