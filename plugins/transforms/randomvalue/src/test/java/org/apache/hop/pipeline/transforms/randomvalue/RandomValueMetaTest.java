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
package org.apache.hop.pipeline.transforms.randomvalue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

public class RandomValueMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testTransformMeta() throws HopException {
    List<String> attributes = Arrays.asList("name", "type");

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("name", "getFieldName");
    getterMap.put("type", "getFieldType");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("name", "setFieldName");
    setterMap.put("type", "setFieldType");

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<>();
    fieldLoadSaveValidatorAttributeMap.put(
        "name", new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 25 ));
    fieldLoadSaveValidatorAttributeMap.put(
        "type",
        new PrimitiveIntArrayLoadSaveValidator(
            new IntLoadSaveValidator(RandomValueMeta.functions.length), 25));

    LoadSaveTester loadSaveTester =
        new LoadSaveTester(
            RandomValueMeta.class,
            attributes,
            getterMap,
            setterMap,
            fieldLoadSaveValidatorAttributeMap,
          new HashMap<>());
    loadSaveTester.testSerialization();
  }
}
