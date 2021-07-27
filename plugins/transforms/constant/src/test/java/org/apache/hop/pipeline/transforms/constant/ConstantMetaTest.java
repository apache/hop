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

package org.apache.hop.pipeline.transforms.constant;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class ConstantMetaTest implements IInitializer<ConstantMeta> {
  LoadSaveTester<ConstantMeta> loadSaveTester;
  Class<ConstantMeta> testMetaClass = ConstantMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init(false);
    List<String> attributes = new ArrayList<>();

    Map<String, String> getterMap = new HashMap<String, String>();
    Map<String, String> setterMap = new HashMap<String, String>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "fields", new ListLoadSaveValidator<>(new ConstantFieldLoadSaveValidator(), 5));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<>(
            testMetaClass,
            attributes,
            new ArrayList<>(),
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();
    validatorFactory.registerValidator(
        validatorFactory.getName(ConstantField.class),
        new ObjectValidator<ConstantField>(
            validatorFactory,
            ConstantField.class,
            Arrays.asList(
                "name",
                "type",
                "format",
                "length",
                "precision",
                "set_empty_string",
                "nullif",
                "group",
                "decimal",
                "currency"),
            new HashMap<String, String>() {
              {
                put("name", "getFieldName");
                put("type", "getFieldType");
                put("format", "getFieldFormat");
                put("length", "getFieldLength");
                put("precision", "getFieldPrecision");
                put("set_empty_string", "isEmptyString");
                put("nullif", "getValue");
                put("group", "getGroup");
                put("decimal", "getDecimal");
                put("currency", "getCurrency");
              }
            },
            new HashMap<String, String>() {
              {
                put("name", "setFieldName");
                put("type", "setFieldType");
                put("format", "setFieldFormat");
                put("length", "setFieldLength");
                put("precision", "setFieldPrecision");
                put("set_empty_string", "setEmptyString");
                put("nullif", "setValue");
                put("group", "setGroup");
                put("decimal", "setDecimal");
                put("currency", "setCurrency");
              }
            }));
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ConstantMeta someMeta) {
    if (someMeta instanceof ConstantMeta) {
      ((ConstantMeta) someMeta).getFields().clear();
      ((ConstantMeta) someMeta)
          .getFields()
          .addAll(
              Arrays.asList(
                  new ConstantField("InField1", "String", "Value1"),
                  new ConstantField("InField2", "String", "Value2"),
                  new ConstantField("InField3", "String", "Value3"),
                  new ConstantField("InField4", "String", "Value4"),
                  new ConstantField("InField5", "String", "Value5")));
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class ConstantFieldLoadSaveValidator implements IFieldLoadSaveValidator<ConstantField> {
    final Random rand = new Random();

    @Override
    public ConstantField getTestObject() {
      String[] types = ValueMetaFactory.getAllValueMetaNames();

      ConstantField field =
          new ConstantField(UUID.randomUUID().toString(), "String", UUID.randomUUID().toString());

      return field;
    }

    @Override
    public boolean validateTestObject(ConstantField testObject, Object actual) {
      if (!(actual instanceof ConstantField)) {
        return false;
      }
      ConstantField another = (ConstantField) actual;
      return new EqualsBuilder()
          .append(testObject.getFieldName(), another.getFieldName())
          .append(testObject.getFieldType(), another.getFieldType())
          .append(testObject.getValue(), another.getValue())
          .isEquals();
    }
  }
}
