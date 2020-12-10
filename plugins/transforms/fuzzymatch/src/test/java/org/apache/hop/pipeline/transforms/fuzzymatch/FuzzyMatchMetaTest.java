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
package org.apache.hop.pipeline.transforms.fuzzymatch;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FuzzyMatchMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  @Before
  public void setUp() throws Exception {
    List<String> attributes =
      Arrays.asList( "value", "valueName", "algorithm", "lookupfield", "mainstreamfield",
        "outputmatchfield", "outputvaluefield", "caseSensitive", "minimalValue",
        "maximalValue", "separator", "closervalue" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "value", "getValue" );
        put( "valueName", "getValueName" );
        put( "algorithm", "getAlgorithmType" );
        put( "lookupfield", "getLookupField" );
        put( "mainstreamfield", "getMainStreamField" );
        put( "outputmatchfield", "getOutputMatchField" );
        put( "outputvaluefield", "getOutputValueField" );
        put( "caseSensitive", "isCaseSensitive" );
        put( "minimalValue", "getMinimalValue" );
        put( "maximalValue", "getMaximalValue" );
        put( "separator", "getSeparator" );
        put( "closervalue", "isGetCloserValue" );
      }
    };

    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "value", "setValue" );
        put( "valueName", "setValueName" );
        put( "algorithm", "setAlgorithmType" );
        put( "lookupfield", "setLookupField" );
        put( "mainstreamfield", "setMainStreamField" );
        put( "outputmatchfield", "setOutputMatchField" );
        put( "outputvaluefield", "setOutputValueField" );
        put( "caseSensitive", "setCaseSensitive" );
        put( "minimalValue", "setMinimalValue" );
        put( "maximalValue", "setMaximalValue" );
        put( "separator", "setSeparator" );
        put( "closervalue", "setGetCloserValue" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 3 );
    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "value", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "valueName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "algorithm", new AlgorithmLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();
    // typeValidatorMap.put( int[].class.getCanonicalName(), new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator(), 3 ) );

    loadSaveTester = new LoadSaveTester( FuzzyMatchMeta.class, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  // Clone test removed as it's covered by the load/save tester now.

  public class AlgorithmLoadSaveValidator implements IFieldLoadSaveValidator<Integer> {
    final Random rand = new Random();

    @Override
    public Integer getTestObject() {
      return rand.nextInt( 10 );
    }

    @Override
    public boolean validateTestObject( Integer testObject, Object actual ) {
      if ( !( actual instanceof Integer ) ) {
        return false;
      }
      Integer actualInt = (Integer) actual;
      return actualInt.equals( testObject );
    }
  }

}
