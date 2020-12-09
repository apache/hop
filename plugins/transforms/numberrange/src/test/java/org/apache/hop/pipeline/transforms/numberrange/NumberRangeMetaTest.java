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

package org.apache.hop.pipeline.transforms.numberrange;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class NumberRangeMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testTransformMeta() throws HopException {
    List<String> attributes = Arrays.asList( "inputField", "outputField", "fallBackValue", "rules" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "inputField", "getInputField" );
    getterMap.put( "outputField", "getOutputField" );
    getterMap.put( "fallBackValue", "getFallBackValue" );
    getterMap.put( "rules", "getRules" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "inputField", "setInputField" );
    setterMap.put( "outputField", "setOutputField" );
    setterMap.put( "fallBackValue", "setFallBackValue" );
    setterMap.put( "rules", "setRules" );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<>();
    fieldLoadSaveValidatorAttributeMap.put( "rules",
      new ListLoadSaveValidator<>( new NumberRangeRuleFieldLoadSaveValidator(), 25 ) );

    LoadSaveTester loadSaveTester = new LoadSaveTester(
      NumberRangeMeta.class, attributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, new HashMap<>() );
    loadSaveTester.testSerialization();
  }

  public class NumberRangeRuleFieldLoadSaveValidator implements IFieldLoadSaveValidator<NumberRangeRule> {
    @Override
    public NumberRangeRule getTestObject() {
      return new NumberRangeRule(
        new Random().nextDouble(),
        new Random().nextDouble(),
        UUID.randomUUID().toString() );
    }

    @Override
    public boolean validateTestObject( NumberRangeRule testObject, Object actual ) {
      return testObject.equals( actual );
    }
  }
}
