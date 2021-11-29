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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class ExecSqlMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<ExecSqlMeta> testMetaClass = ExecSqlMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init(false);
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
            "arguments");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
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
            put("arguments", "getArguments");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
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
            put("arguments", "setArguments");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "arguments", new ListLoadSaveValidator<String>(new StringLoadSaveValidator(), 5));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            new ArrayList<>(),
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof ExecSqlMeta) {
      ((ExecSqlMeta) someMeta).getArguments().clear();
      ((ExecSqlMeta) someMeta).getArguments().addAll(Arrays.asList("a", "b", "c", "d", "e"));
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }
}
