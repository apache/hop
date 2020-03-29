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

package org.apache.hop.trans.steps.analyticquery;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.apache.hop.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.trans.steps.mock.StepMockHelper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalyticQueryMetaTest {

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList( "groupField", "aggregateField", "subjectField",
      "aggregateType", "valueField" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> typeValidators = new HashMap<String, FieldLoadSaveValidator<?>>();
    Map<String, FieldLoadSaveValidator<?>> fieldValidators = new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldValidators.put( "aggregateField", new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 50 ) );
    fieldValidators.put( "subjectField", new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 50 ) );
    fieldValidators.put( "aggregateType", new PrimitiveIntArrayLoadSaveValidator(
      new IntLoadSaveValidator( AnalyticQueryMeta.typeGroupCode.length ), 50 ) );
    fieldValidators.put( "valueField", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator(), 50 ) );

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( AnalyticQueryMeta.class, attributes, getterMap, setterMap, fieldValidators, typeValidators );
    loadSaveTester.testSerialization();
  }


  @Test
  public void testPDI16559() throws Exception {
    StepMockHelper<AnalyticQueryMeta, AnalyticQueryData> mockHelper =
      new StepMockHelper<AnalyticQueryMeta, AnalyticQueryData>( "analyticQuery", AnalyticQueryMeta.class, AnalyticQueryData.class );

    AnalyticQueryMeta analyticQuery = new AnalyticQueryMeta();
    analyticQuery.setGroupField( new String[] { "group1", "group2" } );
    analyticQuery.setSubjectField( new String[] { "field1", "field2", "field3", "field4", "field5" } );
    analyticQuery.setAggregateField( new String[] { "subj1", "subj2", "subj3" } );
    analyticQuery.setAggregateType( new int[] { 0, 1, 2, 3 } );
    analyticQuery.setValueField( new int[] { 0, 4, 8 } );

    try {
      String badXml = analyticQuery.getXML();
      Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    analyticQuery.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = analyticQuery.getXML();

    int targetSz = analyticQuery.getSubjectField().length;

    Assert.assertEquals( targetSz, analyticQuery.getAggregateField().length );
    Assert.assertEquals( targetSz, analyticQuery.getAggregateType().length );
    Assert.assertEquals( targetSz, analyticQuery.getValueField().length );

  }
}
