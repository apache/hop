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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CombinationLookupMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<CombinationLookupMeta> testMetaClass = CombinationLookupMeta.class;
  private IVariables variables;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    variables = new Variables();
    List<String> attributes =
      Arrays.asList( "schemaName", "tableName", "databaseMeta", "replaceFields", "keyField", "keyLookup",
        "useHash", "hashField", "technicalKeyField", "sequenceFrom", "commitSize", "preloadCache", "cacheSize",
        "useAutoinc", "techKeyCreation", "lastUpdateField" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "replaceFields", "replaceFields" );
        put( "useHash", "useHash" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "tableName", "setTablename" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );


    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "keyField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "databaseMeta", new DatabaseMetaLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester = new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
      getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof CombinationLookupMeta ) {
      ( (CombinationLookupMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testProvidesModelerMeta() throws Exception {

    final RowMeta rowMeta = Mockito.mock( RowMeta.class );
    final CombinationLookupMeta combinationLookupMeta = new CombinationLookupMeta() {
      @Override Database createDatabaseObject(IVariables variables) {
        return Mockito.mock( Database.class );
      }

      @Override protected IRowMeta getDatabaseTableFields( Database db, String schemaName, String tableName )
        throws HopDatabaseException {
        assertEquals( "aSchema", schemaName );
        assertEquals( "aDimTable", tableName );
        return rowMeta;
      }
    };
    combinationLookupMeta.setKeyLookup( new String[] { "f1", "f2", "f3" } );
    combinationLookupMeta.setKeyField( new String[] { "s4", "s5", "s6" } );
    combinationLookupMeta.setSchemaName( "aSchema" );
    combinationLookupMeta.setTablename( "aDimTable" );

    final CombinationLookupData dimensionLookupData = new CombinationLookupData();
    assertEquals( rowMeta, combinationLookupMeta.getRowMeta( variables, dimensionLookupData ) );
    assertEquals( 3, combinationLookupMeta.getDatabaseFields().size() );
    assertEquals( "f1", combinationLookupMeta.getDatabaseFields().get( 0 ) );
    assertEquals( "f2", combinationLookupMeta.getDatabaseFields().get( 1 ) );
    assertEquals( "f3", combinationLookupMeta.getDatabaseFields().get( 2 ) );
    assertEquals( 3, combinationLookupMeta.getStreamFields().size() );
    assertEquals( "s4", combinationLookupMeta.getStreamFields().get( 0 ) );
    assertEquals( "s5", combinationLookupMeta.getStreamFields().get( 1 ) );
    assertEquals( "s6", combinationLookupMeta.getStreamFields().get( 2 ) );
  }

  @Test
  public void testPDI16559() throws Exception {
    CombinationLookupMeta combinationLookup = new CombinationLookupMeta();
    combinationLookup.setKeyField( new String[] { "test_field" } );
    combinationLookup.setKeyLookup( new String[] {} );
    combinationLookup.setCacheSize( 15 );
    combinationLookup.setSchemaName( "test_schema" );
    combinationLookup.setTablename( "test_table" );
    combinationLookup.setReplaceFields( true );
    combinationLookup.setPreloadCache( false );

    try {
      String badXml = combinationLookup.getXml();
      Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    combinationLookup.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = combinationLookup.getXml();

    assertEquals( combinationLookup.getKeyField().length, combinationLookup.getKeyLookup().length );

  }

}
