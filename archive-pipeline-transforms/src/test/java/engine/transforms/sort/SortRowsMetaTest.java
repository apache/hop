/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.sort;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveBooleanArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.text.Collator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SortRowsMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  /**
   * Replaced previous testRep with load/save tester. Should cover http://jira.pentaho.com/browse/BACKLOG-377
   *
   * @throws HopException
   */
  @Test
  public void testRoundTrips() throws HopException {
    List<String> attributes = Arrays.asList( "Directory", "Prefix", "SortSize", "FreeMemoryLimit", "CompressFiles",
      "CompressFilesVariable", "OnlyPassingUniqueRows", "FieldName", "Ascending", "CaseSensitive", "CollatorEnabled",
      "CollatorStrength", "PreSortedField" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<String, FieldLoadSaveValidator<?>>();
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 25 );
    FieldLoadSaveValidator<boolean[]> booleanArrayLoadSaveValidator =
      new PrimitiveBooleanArrayLoadSaveValidator( new BooleanLoadSaveValidator(), 25 );
    FieldLoadSaveValidator<int[]> intArrayLoadSaveValidator =
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator(), 25 );

    fieldLoadSaveValidatorAttributeMap.put( "FieldName", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "Ascending", booleanArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "CaseSensitive", booleanArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "CollatorEnabled", booleanArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "CollatorStrength", intArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "PreSortedField", booleanArrayLoadSaveValidator );

    LoadSaveTester<SortRowsMeta> loadSaveTester =
      new LoadSaveTester<>( SortRowsMeta.class, attributes, getterMap, setterMap,
        fieldLoadSaveValidatorAttributeMap, new HashMap<String, FieldLoadSaveValidator<?>>() );

    loadSaveTester.testSerialization();
  }

  @Test
  public void testGetDefaultStrength() {
    SortRowsMeta srm = new SortRowsMeta();
    int usStrength = srm.getDefaultCollationStrength( Locale.US );
    assertEquals( Collator.TERTIARY, usStrength );
    assertEquals( Collator.IDENTICAL, srm.getDefaultCollationStrength( null ) );
  }

  @Test
  public void testPDI16559() throws Exception {
    SortRowsMeta sortRowsReal = new SortRowsMeta();
    SortRowsMeta sortRows = Mockito.spy( sortRowsReal );
    sortRows.setDirectory( "/tmp" );
    sortRows.setFieldName( new String[] { "field1", "field2", "field3", "field4", "field5" } );
    sortRows.setAscending( new boolean[] { false, true, false } );
    sortRows.setCaseSensitive( new boolean[] { true, false, true, false } );
    sortRows.setCollatorEnabled( new boolean[] { false, false, true } );
    sortRows.setCollatorStrength( new int[] { 2, 1, 3 } );
    sortRows.setPreSortedField( new boolean[] { true, true, false } );

    try {
      String badXml = sortRows.getXml();
      Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    sortRows.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = sortRows.getXml();

    int targetSz = sortRows.getFieldName().length;

    Assert.assertEquals( targetSz, sortRows.getAscending().length );
    Assert.assertEquals( targetSz, sortRows.getCaseSensitive().length );
    Assert.assertEquals( targetSz, sortRows.getCollatorEnabled().length );
    Assert.assertEquals( targetSz, sortRows.getCollatorStrength().length );
    Assert.assertEquals( targetSz, sortRows.getPreSortedField().length );

  }
}
