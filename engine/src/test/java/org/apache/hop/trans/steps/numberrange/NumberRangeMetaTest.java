/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.trans.steps.numberrange;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.ListLoadSaveValidator;

public class NumberRangeMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testStepMeta() throws HopException {
    List<String> attributes = Arrays.asList( "inputField", "outputField", "fallBackValue", "rules" );

    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "inputField", "getInputField" );
    getterMap.put( "outputField", "getOutputField" );
    getterMap.put( "fallBackValue", "getFallBackValue" );
    getterMap.put( "rules", "getRules" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "inputField", "setInputField" );
    setterMap.put( "outputField", "setOutputField" );
    setterMap.put( "fallBackValue", "setFallBackValue" );
    setterMap.put( "rules", "setRules" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorAttributeMap.put( "rules",
      new ListLoadSaveValidator<NumberRangeRule>( new NumberRangeRuleFieldLoadSaveValidator(), 25 ) );

    LoadSaveTester loadSaveTester = new LoadSaveTester(
      NumberRangeMeta.class, attributes, getterMap, setterMap,
      fieldLoadSaveValidatorAttributeMap, new HashMap<String, FieldLoadSaveValidator<?>>() );
    loadSaveTester.testSerialization();
  }

  public class NumberRangeRuleFieldLoadSaveValidator implements FieldLoadSaveValidator<NumberRangeRule> {
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
