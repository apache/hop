/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.addsequence;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AddSequenceMetaTest {

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList( "valuename", "useDatabase", "databaseMeta", "schemaName", "sequenceName",
      "useCounter", "counterName", "startAt", "incrementBy", "maxValue" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    getterMap.put( "useDatabase", "isDatabaseUsed" );
    getterMap.put( "useCounter", "isCounterUsed" );

    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<String, IFieldLoadSaveValidator<?>>();
    Map<String, IFieldLoadSaveValidator<?>> fieldValidators = new HashMap<String, IFieldLoadSaveValidator<?>>();

    LoadSaveTester loadSaveTester = new LoadSaveTester( AddSequenceMeta.class, attributes, getterMap, setterMap, fieldValidators, typeValidators );
    loadSaveTester.testSerialization();
  }
}
