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

package org.apache.hop.pipeline.transforms.sortedmerge;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveBooleanArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SortedMergeMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrips() throws HopException {
    List<String> attributes = Arrays.asList( "name", "ascending" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "name", "getFieldName" );
    getterMap.put( "ascending", "getAscending" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "name", "setFieldName" );
    setterMap.put( "ascending", "setAscending" );

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 25 );
    FieldLoadSaveValidator<boolean[]> booleanArrayLoadSaveValidator =
      new PrimitiveBooleanArrayLoadSaveValidator( new BooleanLoadSaveValidator(), 25 );

    fieldLoadSaveValidatorAttributeMap.put( "name", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "ascending", booleanArrayLoadSaveValidator );

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( SortedMergeMeta.class, attributes, getterMap, setterMap,
        fieldLoadSaveValidatorAttributeMap, new HashMap<String, FieldLoadSaveValidator<?>>() );

    loadSaveTester.testSerialization();
  }

  @Test
  public void testPDI16559() throws Exception {
    SortedMergeMeta sortedMerge = new SortedMergeMeta();
    sortedMerge.setFieldName( new String[] { "field1", "field2", "field3", "field4", "field5" } );
    sortedMerge.setAscending( new boolean[] { false, true } );

    try {
      String badXml = sortedMerge.getXML();
      Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    sortedMerge.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = sortedMerge.getXML();

    int targetSz = sortedMerge.getFieldName().length;

    Assert.assertEquals( targetSz, sortedMerge.getAscending().length );

  }
}
