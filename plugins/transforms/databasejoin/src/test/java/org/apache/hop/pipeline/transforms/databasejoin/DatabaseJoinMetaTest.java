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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class DatabaseJoinMetaTest implements IInitializer<DatabaseJoinMeta> {
  LoadSaveTester<DatabaseJoinMeta> loadSaveTester;
  Class<DatabaseJoinMeta> testMetaClass = DatabaseJoinMeta.class;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Before
  public void setUpLoadSave() throws Exception {
    List<String> attributes =
        Arrays.asList(
            "sql", "rowLimit", "outerJoin", "replaceVariables", "connection", "parameters");

    Map<String, String> getterMap = new HashMap<>();
    //    getterMap.put("parameters", "getParameters");
    //    getterMap.put("databaseMeta", "getDatabaseMeta");

    Map<String, String> setterMap = new HashMap<>();
    // setterMap.put("parameters", "setParameters");

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
  public void modify(DatabaseJoinMeta someMeta) {}

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class ParameterFieldLoadSaveValidator implements IFieldLoadSaveValidator<ParameterField> {
    final Random rand = new Random();

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
