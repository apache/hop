/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.uniquerowsbyhashset;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UniqueRowsByHashSetMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes =
      Arrays.asList( "store_values", "reject_duplicate_row", "error_description", "name" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "store_values", "getStoreValues" );
    getterMap.put( "reject_duplicate_row", "isRejectDuplicateRow" );
    getterMap.put( "error_description", "getErrorDescription" );
    getterMap.put( "name", "getCompareFields" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "store_values", "setStoreValues" );
    setterMap.put( "reject_duplicate_row", "setRejectDuplicateRow" );
    setterMap.put( "error_description", "setErrorDescription" );
    setterMap.put( "name", "setCompareFields" );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<String, IFieldLoadSaveValidator<?>>();

    //Arrays need to be consistent length
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 25 );

    fieldLoadSaveValidatorAttributeMap.put( "name", stringArrayLoadSaveValidator );

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( UniqueRowsByHashSetMeta.class, attributes, getterMap, setterMap,
        fieldLoadSaveValidatorAttributeMap, new HashMap<String, IFieldLoadSaveValidator<?>>() );

    loadSaveTester.testSerialization();
  }
}
