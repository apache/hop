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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.NonZeroIntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DimensionLookupMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<DimensionLookupMeta> testMetaClass = DimensionLookupMeta.class;
  private ThreadLocal<DimensionLookupMeta> holdTestingMeta = new ThreadLocal<>();
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private IVariables variables;

  @BeforeClass
  public static void setupClass() throws HopException {
    HopEnvironment.init();
  }

  @Before
  public void setUpLoadSave() throws Exception {
    variables = new Variables();
    List<String> attributes =
      Arrays.asList( "schemaName", "tableName", "update", "dateField", "dateFrom", "dateTo", "keyField", "keyRename",
        "autoIncrement", "versionField", "commitSize", "useBatchUpdate", "minYear", "maxYear", "techKeyCreation",
        "cacheSize", "usingStartDateAlternative", "startDateAlternative", "startDateFieldName", "preloadingCache", "keyStream",
        "keyLookup", "fieldStream", "fieldLookup", "fieldUpdate", "databaseMeta", "sequenceName" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "useBatchUpdate", "useBatchUpdate" );
      }
    };
    Map<String, String> setterMap = new HashMap<>();

    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "keyStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldLookup", stringArrayLoadSaveValidator );
    // Note - have to use the non-zero int load/save validator here because if "update"
    // is false, code in DimensionLookupMeta replaces "IValueMeta.TYPE_NONE" with
    // IValueMeta.TYPE_STRING. This happens about once out of every 3 or so runs of
    // the test which made it a bit difficult to track down.
    // MB - 5/2016
    attrValidatorMap.put( "fieldUpdate", new FieldUpdateIntArrayLoadSaveValidator( new NonZeroIntLoadSaveValidator(
      DimensionLookupMeta.typeDesc.length ), 5 ) );
    attrValidatorMap.put( "databaseMeta", new DatabaseMetaLoadSaveValidator() );
    attrValidatorMap.put( "startDateAlternative", new IntLoadSaveValidator( DimensionLookupMeta.getStartDateAlternativeCodes().length ) );
    attrValidatorMap.put( "sequenceName", new SequenceNameLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester = new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof DimensionLookupMeta ) {
      ( (DimensionLookupMeta) someMeta ).allocate( 5, 5 );
      // doing this as a work-around for sequenceName validation.
      // Apparently, sequenceName will always be written (getXml),
      // but will only be read if the value of "update" is true.
      // While testing the load/save behavior, there is no sane way
      // to test dependent variables like this (that I could see). So,
      // I'm holding onto the meta, and will have a special load/save handler
      // for sequenceName.
      // MB - 5/2016
      this.holdTestingMeta.set( (DimensionLookupMeta) someMeta );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }


  @Before
  public void setUp() throws Exception {
    ILogChannelFactory logChannelFactory = mock( ILogChannelFactory.class );
    ILogChannel logChannelInterface = mock( ILogChannel.class );
    HopLogStore.setLogChannelFactory( logChannelFactory );
    when( logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      logChannelInterface );
  }

  @Test
  public void testGetFields() throws Exception {

    RowMeta extraFields = new RowMeta();
    extraFields.addValueMeta( new ValueMetaString( "field1" ) );

    DatabaseMeta dbMeta = mock( DatabaseMeta.class );

    DimensionLookupMeta meta = spy( new DimensionLookupMeta() );
    meta.setUpdate( false );
    meta.setKeyField( null );
    meta.setFieldLookup( new String[] { "field1" } );
    meta.setFieldStream( new String[] { "" } );
    meta.setDatabaseMeta( dbMeta );
    doReturn( extraFields ).when( meta ).getDatabaseTableFields( (Database) anyObject(), anyString(), anyString() );
    doReturn( mock( ILogChannel.class ) ).when( meta ).getLog();

    RowMeta row = new RowMeta();
    try {
      meta.getFields( row, "DimensionLookupMetaTest", new RowMeta[] { row }, null, null, null );
    } catch ( Throwable e ) {
      Assert.assertTrue( e.getMessage().contains(
        BaseMessages.getString( DimensionLookupMeta.class, "DimensionLookupMeta.Error.NoTechnicalKeySpecified" ) ) );
    }
  }

  @Test
  public void testProvidesModelerMeta() throws Exception {

    final RowMeta rowMeta = Mockito.mock( RowMeta.class );
    final DimensionLookupMeta dimensionLookupMeta = new DimensionLookupMeta() {
      @Override Database createDatabaseObject(IVariables variables) {
        return mock( Database.class );
      }

      @Override protected IRowMeta getDatabaseTableFields( Database db, String schemaName, String tableName )
        throws HopDatabaseException {
        assertEquals( "aSchema", schemaName );
        assertEquals( "aDimTable", tableName );
        return rowMeta;
      }
    };
    dimensionLookupMeta.setFieldLookup( new String[] { "f1", "f2", "f3" } );
    dimensionLookupMeta.setKeyLookup( new String[] { "k1" } );
    dimensionLookupMeta.setFieldStream( new String[] { "s4", "s5", "s6" } );
    dimensionLookupMeta.setKeyStream( new String[] { "ks1" } );
    dimensionLookupMeta.setSchemaName( "aSchema" );
    dimensionLookupMeta.setTableName( "aDimTable" );

    final DimensionLookupData dimensionLookupData = new DimensionLookupData();
    assertEquals( rowMeta, dimensionLookupMeta.getRowMeta( variables, dimensionLookupData ) );
    assertEquals( 4, dimensionLookupMeta.getDatabaseFields().size() );
    assertEquals( "f1", dimensionLookupMeta.getDatabaseFields().get( 0 ) );
    assertEquals( "f2", dimensionLookupMeta.getDatabaseFields().get( 1 ) );
    assertEquals( "f3", dimensionLookupMeta.getDatabaseFields().get( 2 ) );
    assertEquals( "k1", dimensionLookupMeta.getDatabaseFields().get( 3 ) );
    assertEquals( 4, dimensionLookupMeta.getStreamFields().size() );
    assertEquals( "s4", dimensionLookupMeta.getStreamFields().get( 0 ) );
    assertEquals( "s5", dimensionLookupMeta.getStreamFields().get( 1 ) );
    assertEquals( "s6", dimensionLookupMeta.getStreamFields().get( 2 ) );
    assertEquals( "ks1", dimensionLookupMeta.getStreamFields().get( 3 ) );
  }

  // Note - Removed cloneTest since it's covered by the load/save tester

  // Doing this as a work-around for sequenceName validation.
  // Apparently, sequenceName will always be written (getXml),
  // but will only be read if the value of "update" is true (readData).
  // While testing the load/save behavior, there is no sane way
  // to test dependent variables like this (that I could see). So,
  // I'm holding onto the meta in a threadlocal, and have to have
  // this special load/save handler for sequenceName.
  // MB - 5/2016
  public class SequenceNameLoadSaveValidator implements IFieldLoadSaveValidator<String> {
    final Random rand = new Random();

    @Override
    public String getTestObject() {
      DimensionLookupMeta dlm = holdTestingMeta.get(); // get the currently-being tested meta
      if ( dlm.isUpdate() ) { // value returned here is dependant on isUpdate()
        return UUID.randomUUID().toString(); // return a string
      } else {
        return null; // Return null if !isUpdate ...
      }
    }

    @Override
    public boolean validateTestObject( String testObject, Object actual ) {
      String another = (String) actual;
      DimensionLookupMeta dlm = holdTestingMeta.get();
      if ( dlm.isUpdate() ) {
        return testObject.equals( another ); // if isUpdate, compare strings
      } else {
        return ( another == null ); // If !isUpdate, another should be null
      }
    }
  }

  public class FieldUpdateIntArrayLoadSaveValidator extends PrimitiveIntArrayLoadSaveValidator {

    public FieldUpdateIntArrayLoadSaveValidator( IFieldLoadSaveValidator<Integer> fieldValidator ) {
      this( fieldValidator, null );
    }

    public FieldUpdateIntArrayLoadSaveValidator( IFieldLoadSaveValidator<Integer> fieldValidator, Integer elements ) {
      super( fieldValidator, elements );
    }

    @Override
    public int[] getTestObject() {
      DimensionLookupMeta dlm = holdTestingMeta.get();
      int[] testObject = super.getTestObject();
      if ( !dlm.isUpdate() ) {
        dlm.setReturnType( testObject );
      }
      return testObject;
    }
  }


  @Test
  public void testPDI16559() throws Exception {
    DimensionLookupMeta dimensionLookup = new DimensionLookupMeta();
    dimensionLookup.setKeyStream( new String[] { "test_field" } );
    dimensionLookup.setKeyLookup( new String[] {} );
    dimensionLookup.setCacheSize( 15 );
    dimensionLookup.setSchemaName( "test_schema" );
    dimensionLookup.setFieldStream( new String[] { "123", "abc", "def" } );
    dimensionLookup.setFieldLookup( new String[] { "wibble" } );
    dimensionLookup.setFieldUpdate( new int[] { 11, 12 } );

    try {
      String badXml = dimensionLookup.getXml();
      Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    dimensionLookup.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = dimensionLookup.getXml();

    Assert.assertEquals( dimensionLookup.getKeyStream().length, dimensionLookup.getKeyLookup().length );
    Assert.assertEquals( dimensionLookup.getFieldStream().length, dimensionLookup.getFieldLookup().length );
    Assert.assertEquals( dimensionLookup.getFieldUpdate().length, dimensionLookup.getFieldUpdate().length );

  }

}
