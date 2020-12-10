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

package org.apache.hop.pipeline.transforms.salesforceinput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.TransformLoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnectionUtils;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceMetaTest;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SalesforceInputMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( true );
    String passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.HOP_PASSWORD_ENCODER_PLUGIN ), "Hop" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test
  public void testErrorHandling() {
    SalesforceTransformMeta meta = new SalesforceInputMeta();
    assertFalse( meta.supportsErrorHandling() );
  }

  @Test
  public void testSalesforceInputMeta() throws HopException {
    List<String> attributes = new ArrayList<>();
    attributes.addAll( SalesforceMetaTest.getDefaultAttributes() );
    attributes.addAll( Arrays.asList( "inputFields", "condition", "query", "specifyQuery", "includeTargetURL",
      "targetURLField", "includeModule", "moduleField", "includeRowNumber", "includeDeletionDate", "deletionDateField",
      "rowNumberField", "includeSQL", "sqlField", "includeTimestamp", "timestampField", "readFrom", "readTo",
      "recordsFilter", "queryAll", "rowLimit" ) );
    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    getterMap.put( "includeTargetURL", "includeTargetURL" );
    getterMap.put( "includeModule", "includeModule" );
    getterMap.put( "includeRowNumber", "includeRowNumber" );
    getterMap.put( "includeDeletionDate", "includeDeletionDate" );
    getterMap.put( "includeSQL", "includeSQL" );
    getterMap.put( "sqlField", "getSQLField" );
    setterMap.put( "sqlField", "setSQLField" );
    getterMap.put( "includeTimestamp", "includeTimestamp" );


    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidators = new HashMap<>();
    fieldLoadSaveValidators.put( "inputFields",
      new ArrayLoadSaveValidator<>( new SalesforceInputFieldLoadSaveValidator(), 50 ) );
    fieldLoadSaveValidators.put( "recordsFilter", new RecordsFilterLoadSaveValidator() );

    TransformLoadSaveTester<SalesforceInputMeta> transformLoadSaveTester =
      new TransformLoadSaveTester( SalesforceInputMeta.class, attributes, attributes, getterMap, setterMap,
        fieldLoadSaveValidators, new HashMap<>() );

    transformLoadSaveTester.testXmlRoundTrip();
  }

  @Test
  public void testGetFields() throws HopTransformException {
    SalesforceInputMeta meta = new SalesforceInputMeta();
    meta.setDefault();
    IRowMeta r = new RowMeta();
    meta.getFields( r, "thisTransform", null, null, new Variables(), null );
    assertEquals( 0, r.size() );

    meta.setInputFields( new SalesforceInputField[]{ new SalesforceInputField( "field1" ) } );
    r.clear();
    meta.getFields( r, "thisTransform", null, null, new Variables(), null );
    assertEquals( 1, r.size() );

    meta.setIncludeDeletionDate( true );
    meta.setDeletionDateField( "DeletionDate" );
    meta.setIncludeModule( true );
    meta.setModuleField( "ModuleName" );
    meta.setIncludeRowNumber( true );
    meta.setRowNumberField( "RN" );
    meta.setIncludeSQL( true );
    meta.setSQLField( "sqlField" );
    meta.setIncludeTargetURL( true );
    meta.setTargetURLField( "Target" );
    meta.setIncludeTimestamp( true );
    meta.setTimestampField( "TS" );
    r.clear();
    meta.getFields( r, "thisTransform", null, null, new Variables(), null );
    assertEquals( 7, r.size() );
    assertTrue( r.indexOfValue( "field1" ) >= 0 );
    assertTrue( r.indexOfValue( "DeletionDate" ) >= 0 );
    assertTrue( r.indexOfValue( "ModuleName" ) >= 0 );
    assertTrue( r.indexOfValue( "RN" ) >= 0 );
    assertTrue( r.indexOfValue( "sqlField" ) >= 0 );
    assertTrue( r.indexOfValue( "Target" ) >= 0 );
    assertTrue( r.indexOfValue( "TS" ) >= 0 );
  }

  @Test
  public void testCheck() {
    SalesforceInputMeta meta = new SalesforceInputMeta();
    meta.setDefault();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check( remarks, null, null, null, null, null, null, null, null );
    boolean hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertTrue( hasError );

    remarks.clear();
    meta.setDefault();
    meta.setUsername( "user" );
    meta.setInputFields( new SalesforceInputField[]{ new SalesforceInputField( "test" ) } );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertFalse( hasError );

    remarks.clear();
    meta.setDefault();
    meta.setUsername( "user" );
    meta.setIncludeDeletionDate( true );
    meta.setIncludeModule( true );
    meta.setIncludeRowNumber( true );
    meta.setIncludeSQL( true );
    meta.setIncludeTargetURL( true );
    meta.setIncludeTimestamp( true );
    meta.setInputFields( new SalesforceInputField[]{ new SalesforceInputField( "test" ) } );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    int errorCount = 0;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
        errorCount++;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertTrue( hasError );
    assertEquals( 6, errorCount );

    remarks.clear();
    meta.setDefault();
    meta.setUsername( "user" );
    meta.setIncludeDeletionDate( true );
    meta.setDeletionDateField( "delDate" );
    meta.setIncludeModule( true );
    meta.setModuleField( "mod" );
    meta.setIncludeRowNumber( true );
    meta.setRowNumberField( "rownum" );
    meta.setIncludeSQL( true );
    meta.setSQLField( "theSQL" );
    meta.setIncludeTargetURL( true );
    meta.setTargetURLField( "theURL" );
    meta.setIncludeTimestamp( true );
    meta.setTimestampField( "ts_Field" );
    meta.setInputFields( new SalesforceInputField[]{ new SalesforceInputField( "test" ) } );
    meta.check( remarks, null, null, null, null, null, null, null, null );
    hasError = false;
    for ( ICheckResult cr : remarks ) {
      if ( cr.getType() == CheckResult.TYPE_RESULT_ERROR ) {
        hasError = true;
        errorCount++;
      }
    }
    assertFalse( remarks.isEmpty() );
    assertFalse( hasError );
  }

  public static class RecordsFilterLoadSaveValidator extends IntLoadSaveValidator {
    @Override
    public Integer getTestObject() {
      return new Random().nextInt( SalesforceConnectionUtils.recordsFilterCode.length );
    }
  }

  public static class SalesforceInputFieldLoadSaveValidator implements IFieldLoadSaveValidator<SalesforceInputField> {
    static final Random rnd = new Random();

    @Override
    public SalesforceInputField getTestObject() {
      SalesforceInputField retval = new SalesforceInputField();
      retval.setName( UUID.randomUUID().toString() );
      retval.setField( UUID.randomUUID().toString() );
      retval.setIdLookup( rnd.nextBoolean() );
      retval.setType( rnd.nextInt( ValueMetaFactory.getAllValueMetaNames().length ) );
      retval.setFormat( UUID.randomUUID().toString() );
      retval.setCurrencySymbol( UUID.randomUUID().toString() );
      retval.setDecimalSymbol( UUID.randomUUID().toString() );
      retval.setGroupSymbol( UUID.randomUUID().toString() );
      retval.setLength( rnd.nextInt() );
      retval.setPrecision( rnd.nextInt() );
      retval.setTrimType( rnd.nextInt( SalesforceInputField.trimTypeCode.length ) );
      retval.setRepeated( rnd.nextBoolean() );
      return retval;
    }

    @Override
    public boolean validateTestObject( SalesforceInputField testObject, Object actual ) {
      if ( !( actual instanceof SalesforceInputField ) ) {
        return false;
      }
      SalesforceInputField sfActual = (SalesforceInputField) actual;
      if ( !sfActual.getName().equals( testObject.getName() )
          || !sfActual.getField().equals( testObject.getField() )
          || sfActual.isIdLookup() != testObject.isIdLookup()
          || sfActual.getType() != testObject.getType()
          || !sfActual.getFormat().equals( testObject.getFormat() )
          || !sfActual.getCurrencySymbol().equals( testObject.getCurrencySymbol() )
          || !sfActual.getDecimalSymbol().equals( testObject.getDecimalSymbol() )
          || !sfActual.getGroupSymbol().equals( testObject.getGroupSymbol() )
          || sfActual.getLength() != testObject.getLength()
          || sfActual.getPrecision() != testObject.getPrecision()
          || sfActual.getTrimType() != testObject.getTrimType()
          || sfActual.isRepeated() != testObject.isRepeated() ) {
        return false;
      }
      return true;
    }
  }
}
