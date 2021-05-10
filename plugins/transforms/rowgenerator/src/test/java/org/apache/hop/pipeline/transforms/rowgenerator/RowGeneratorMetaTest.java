/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.rowgenerator;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class RowGeneratorMetaTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private final String launchVariable = "${ROW_LIMIT}";

  private final String rowGeneratorRowLimitCode = "limit";
  private LoadSaveTester<?> loadSaveTester;
  private Class<RowGeneratorMeta> testMetaClass = RowGeneratorMeta.class;

  @Before
  public void setUp() throws HopException {}

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init(false);
    List<String> attributes =
        Arrays.asList(
            "neverEnding", "intervalInMs", "rowTimeField", "lastTimeField", "rowLimit", "fields");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("neverEnding", "isNeverEnding");
            put("intervalInMs", "getIntervalInMs");
            put("rowTimeField", "getRowTimeField");
            put("lastTimeField", "getLastTimeField");
            put("rowLimit", "getRowLimit");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
          {
            put("neverEnding", "setNeverEnding");
            put("intervalInMs", "setIntervalInMs");
            put("rowTimeField", "setRowTimeField");
            put("lastTimeField", "setLastTimeField");
            put("rowLimit", "setRowLimit");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "fields", new ListLoadSaveValidator<>(new GeneratorFieldInputFieldLoadSaveValidator(), 5));

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
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof RowGeneratorMeta) {
      ((RowGeneratorMeta) someMeta).getFields().clear();
      ((RowGeneratorMeta) someMeta)
          .getFields()
          .addAll(
              Arrays.asList(
                  new GeneratorField("a", "String", null, 50, -1, null, null, null, "AAAA", false),
                  new GeneratorField("b", "String", null, 50, -1, null, null, null, "BBBB", false),
                  new GeneratorField("c", "String", null, 50, -1, null, null, null, "CCCC", false),
                  new GeneratorField("d", "String", null, 50, -1, null, null, null, "DDDD", false),
                  new GeneratorField(
                      "e", "String", null, 50, -1, null, null, null, "EEEE", false)));
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class GeneratorFieldInputFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<GeneratorField> {
    final Random rand = new Random();

    @Override
    public GeneratorField getTestObject() {
      String[] types = ValueMetaFactory.getAllValueMetaNames();

      GeneratorField field =
          new GeneratorField(
              UUID.randomUUID().toString(),
              types[Math.abs(rand.nextInt(types.length))],
              UUID.randomUUID().toString(),
              rand.nextInt(20),
              rand.nextInt(20),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              rand.nextInt(20) < 0);

      return field;
    }

    @Override
    public boolean validateTestObject(GeneratorField testObject, Object actual) {
      if (!(actual instanceof GeneratorField)) {
        return false;
      }
      GeneratorField another = (GeneratorField) actual;
      return new EqualsBuilder()
          .append(testObject.getName(), another.getName())
          .append(testObject.getType(), another.getType())
          .append(testObject.getFormat(), another.getFormat())
          .append(testObject.getLength(), another.getLength())
          .append(testObject.getPrecision(), another.getPrecision())
          .append(testObject.getDecimal(), another.getDecimal())
          .append(testObject.getGroup(), another.getGroup())
          .append(testObject.getValue(), another.getValue())
          .append(testObject.isSetEmptyString(), another.isSetEmptyString())
          .isEquals();
    }
  }
}
