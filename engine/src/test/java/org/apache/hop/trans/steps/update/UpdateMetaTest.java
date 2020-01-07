/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.trans.steps.update;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;

import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.apache.hop.trans.steps.loadsave.initializer.InitializerInterface;
import org.apache.hop.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.trans.steps.mock.StepMockHelper;
import org.apache.hop.metastore.api.IMetaStore;


public class UpdateMetaTest implements InitializerInterface<StepMetaInterface> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private StepMeta stepMeta;
  private Update upd;
  private UpdateData ud;
  private UpdateMeta umi;
  LoadSaveTester loadSaveTester;
  Class<UpdateMeta> testMetaClass = UpdateMeta.class;
  private StepMockHelper<UpdateMeta, UpdateData> mockHelper;

  @Before
  public void setUp() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init( false );
    TransMeta transMeta = new TransMeta();
    transMeta.setName( "delete1" );

    Map<String, String> vars = new HashMap<String, String>();
    vars.put( "max.sz", "10" );
    transMeta.injectVariables( vars );

    umi = new UpdateMeta();
    ud = new UpdateData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId( StepPluginType.class, umi );

    stepMeta = new StepMeta( deletePid, "delete", umi );
    Trans trans = new Trans( transMeta );
    transMeta.addStep( stepMeta );
    mockHelper = new StepMockHelper<>( "Update", UpdateMeta.class, UpdateData.class );
    Mockito.when( mockHelper.logChannelInterfaceFactory.create( Mockito.any(), Mockito.any( LoggingObjectInterface.class ) ) ).thenReturn( mockHelper.logChannelInterface );

    upd = new Update( stepMeta, ud, 1, transMeta, trans );
    upd.copyVariablesFrom( transMeta );

    List<String> attributes =
        Arrays.asList( "schemaName", "tableName", "commitSize", "errorIgnored", "ignoreFlagField",
            "skipLookup", "useBatchUpdate", "keyStream", "keyLookup", "keyCondition", "keyStream2",
            "updateLookup", "updateStream", "databaseMeta" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "schemaName", "getSchemaName" );
        put( "tableName", "getTableName" );
        put( "commitSize", "getCommitSizeVar" );
        put( "errorIgnored", "isErrorIgnored" );
        put( "ignoreFlagField", "getIgnoreFlagField" );
        put( "skipLookup", "isSkipLookup" );
        put( "useBatchUpdate", "useBatchUpdate" );
        put( "keyStream", "getKeyStream" );
        put( "keyLookup", "getKeyLookup" );
        put( "keyCondition", "getKeyCondition" );
        put( "keyStream2", "getKeyStream2" );
        put( "updateLookup", "getUpdateLookup" );
        put( "updateStream", "getUpdateStream" );
        put( "databaseMeta", "getDatabaseMeta" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "schemaName", "setSchemaName" );
        put( "tableName", "setTableName" );
        put( "commitSize", "setCommitSize" );
        put( "errorIgnored", "setErrorIgnored" );
        put( "ignoreFlagField", "setIgnoreFlagField" );
        put( "skipLookup", "setSkipLookup" );
        put( "useBatchUpdate", "setUseBatchUpdate" );
        put( "keyStream", "setKeyStream" );
        put( "keyLookup", "setKeyLookup" );
        put( "keyCondition", "setKeyCondition" );
        put( "keyStream2", "setKeyStream2" );
        put( "updateLookup", "setUpdateLookup" );
        put( "updateStream", "setUpdateStream" );
        put( "databaseMeta", "setDatabaseMeta" );
      }
    };
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );


    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "keyStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyCondition", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyStream2", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "updateLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "updateStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "databaseMeta", new DatabaseMetaLoadSaveValidator() );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
        new LoadSaveTester( testMetaClass, attributes, new ArrayList<String>(), new ArrayList<String>(),
            getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void testCommitCountFixed() {
    umi.setCommitSize( "100" );
    assertTrue( umi.getCommitSize( upd ) == 100 );
  }

  @Test
  public void testCommitCountVar() {
    umi.setCommitSize( "${max.sz}" );
    assertTrue( umi.getCommitSize( upd ) == 10 );
  }

  @Test
  public void testCommitCountMissedVar() {
    umi.setCommitSize( "missed-var" );
    try {
      umi.getCommitSize( upd );
      fail();
    } catch ( Exception ex ) {
    }
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( StepMetaInterface someMeta ) {
    if ( someMeta instanceof UpdateMeta ) {
      ( (UpdateMeta) someMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testPDI16559() throws Exception {
    UpdateMeta update = new UpdateMeta();
    update.setKeyStream( new String[] { "field1", "field2", "field3", "field4", "field5" } );
    update.setKeyLookup( new String[] { "lkup1", "lkup2" } );
    update.setKeyCondition( new String[] { "cond1", "cond2", "cond3" } );
    update.setKeyStream2( new String[] { "str21", "str22", "str23", "str24" } );

    update.setUpdateLookup( new String[] { "updlkup1", "updlkup2", "updlkup3", "updlkup4" } );
    update.setUpdateStream( new String[] { "updlkup1", "updlkup2" } );

    try {
      String badXml = update.getXML();
      fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    update.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = update.getXML();

    int targetSz = update.getKeyStream().length;

    assertEquals( targetSz, update.getKeyLookup().length );
    assertEquals( targetSz, update.getKeyCondition().length );
    assertEquals( targetSz, update.getKeyStream2().length );

    targetSz = update.getUpdateLookup().length;
    assertEquals( targetSz, update.getUpdateStream().length );

  }
}
