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
package org.apache.hop.pipeline.transforms.stringcut;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class StringCutMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<StringCutMeta> testMetaClass = StringCutMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();

    List<String> attributesList = new ArrayList<>();
    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "fields", new ListLoadSaveValidator<>(new StringCutFieldInputFieldLoadSaveValidator(), 5));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributesList,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof StringCutMeta) {
      ((StringCutMeta) someMeta).getFields().clear();
      ((StringCutMeta) someMeta)
          .getFields()
          .addAll(
              Arrays.asList(
                  new StringCutField("InField1", "OutField1", "1", "10"),
                  new StringCutField("InField2", "OutField2", "2", "20"),
                  new StringCutField("InField3", "OutField3", "1", "10"),
                  new StringCutField("InField4", "OutField4", "1", "10"),
                  new StringCutField("InField5", "OutField5", "1", "10")));
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class StringCutFieldInputFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<StringCutField> {
    final Random rand = new Random();

    @Override
    public StringCutField getTestObject() {
      String[] types = ValueMetaFactory.getAllValueMetaNames();

      StringCutField field =
          new StringCutField(UUID.randomUUID().toString(), UUID.randomUUID().toString(), "1", "5");

      return field;
    }

    @Override
    public boolean validateTestObject(StringCutField testObject, Object actual) {
      if (!(actual instanceof StringCutField)) {
        return false;
      }
      StringCutField another = (StringCutField) actual;
      return new EqualsBuilder()
          .append(testObject.getFieldInStream(), another.getFieldInStream())
          .append(testObject.getFieldOutStream(), another.getFieldOutStream())
          .append(testObject.getCutFrom(), another.getCutFrom())
          .append(testObject.getCutTo(), another.getCutTo())
          .isEquals();
    }
  }
}
