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

package org.apache.hop.pipeline.transforms.insertupdate;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveBooleanArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertUpdateMetaTest {
  LoadSaveTester loadSaveTester;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private IVariables variables;
  private TransformMeta transformMeta;
  private InsertUpdate upd;
  private InsertUpdateData ud;
  private InsertUpdateMeta umi;
  private TransformMockHelper<InsertUpdateMeta, InsertUpdateData> mockHelper;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    variables = new Variables();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "delete1" );

    umi = new InsertUpdateMeta();
    ud = new InsertUpdateData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId( TransformPluginType.class, umi );

    transformMeta = new TransformMeta( deletePid, "delete", umi );
    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );

    Map<String, String> vars = new HashMap<>();
    vars.put( "max.sz", "10" );
    pipeline.setVariables( vars );

    pipelineMeta.addTransform( transformMeta );
    upd = new InsertUpdate( transformMeta, umi, ud, 1, pipelineMeta, pipeline );

    mockHelper =
      new TransformMockHelper<>( "insertUpdate", InsertUpdateMeta.class, InsertUpdateData.class );
    Mockito.when( mockHelper.logChannelFactory.create( Mockito.any(), Mockito.any( ILoggingObject.class ) ) )
      .thenReturn( mockHelper.iLogChannel );
    Mockito.when( mockHelper.transformMeta.getTransform() ).thenReturn( new InsertUpdateMeta() );
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void testCommitCountFixed() {
    umi.setCommitSize( "100" );
    Assert.assertTrue( umi.getCommitSize( upd ) == 100 );
  }

  @Test
  public void testCommitCountVar() {
    umi.setCommitSize( "${max.sz}" );
    Assert.assertTrue( umi.getCommitSize( upd ) == 10 );
  }

  @Test
  public void testProvidesModeler() throws Exception {
    InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
    insertUpdateMeta.setUpdateLookup( new String[] { "f1", "f2", "f3" } );
    insertUpdateMeta.setUpdateStream( new String[] { "s4", "s5", "s6" } );

    InsertUpdateData tableOutputData = new InsertUpdateData();
    tableOutputData.insertRowMeta = Mockito.mock( RowMeta.class );
    Assert.assertEquals( tableOutputData.insertRowMeta, insertUpdateMeta.getRowMeta( variables, tableOutputData ) );
    Assert.assertEquals( 3, insertUpdateMeta.getDatabaseFields().size() );
    Assert.assertEquals( "f1", insertUpdateMeta.getDatabaseFields().get( 0 ) );
    Assert.assertEquals( "f2", insertUpdateMeta.getDatabaseFields().get( 1 ) );
    Assert.assertEquals( "f3", insertUpdateMeta.getDatabaseFields().get( 2 ) );
    Assert.assertEquals( 3, insertUpdateMeta.getStreamFields().size() );
    Assert.assertEquals( "s4", insertUpdateMeta.getStreamFields().get( 0 ) );
    Assert.assertEquals( "s5", insertUpdateMeta.getStreamFields().get( 1 ) );
    Assert.assertEquals( "s6", insertUpdateMeta.getStreamFields().get( 2 ) );
  }

  @Test
  public void testCommitCountMissedVar() {
    umi.setCommitSize( "missed-var" );
    try {
      umi.getCommitSize( upd );
      Assert.fail();
    } catch ( Exception ex ) {
    }
  }

  @Before
  public void setUpLoadSave() throws Exception {
    List<String> attributes =
      Arrays.asList( "schemaName", "tableName", "databaseMeta", "keyStream", "keyLookup", "keyCondition",
        "keyStream2", "updateLookup", "updateStream", "update", "commitSize", "updateBypassed" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "schemaName", "getSchemaName" );
        put( "tableName", "getTableName" );
        put( "databaseMeta", "getDatabaseMeta" );
        put( "keyStream", "getKeyStream" );
        put( "keyLookup", "getKeyLookup" );
        put( "keyCondition", "getKeyCondition" );
        put( "keyStream2", "getKeyStream2" );
        put( "updateLookup", "getUpdateLookup" );
        put( "updateStream", "getUpdateStream" );
        put( "update", "getUpdate" );
        put( "commitSize", "getCommitSizeVar" );
        put( "updateBypassed", "isUpdateBypassed" );
      }
    };

    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "schemaName", "setSchemaName" );
        put( "tableName", "setTableName" );
        put( "databaseMeta", "setDatabaseMeta" );
        put( "keyStream", "setKeyStream" );
        put( "keyLookup", "setKeyLookup" );
        put( "keyCondition", "setKeyCondition" );
        put( "keyStream2", "setKeyStream2" );
        put( "updateLookup", "setUpdateLookup" );
        put( "updateStream", "setUpdateStream" );
        put( "update", "setUpdate" );
        put( "commitSize", "setCommitSize" );
        put( "updateBypassed", "setUpdateBypassed" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "keyStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyCondition", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyStream2", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "updateLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "updateStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "databaseMeta", new DatabaseMetaLoadSaveValidator() );
    attrValidatorMap.put( "update", new ArrayLoadSaveValidator<>( new BooleanLoadSaveValidator(), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    typeValidatorMap.put( boolean[].class.getCanonicalName(), new PrimitiveBooleanArrayLoadSaveValidator( new BooleanLoadSaveValidator(), 3 ) );

    loadSaveTester = new LoadSaveTester( InsertUpdateMeta.class, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testErrorProcessRow() throws HopException {
    Mockito.when( mockHelper.logChannelFactory.create( Mockito.any(), Mockito.any( ILoggingObject.class ) ) )
      .thenReturn(
        mockHelper.iLogChannel );
    Mockito.when( mockHelper.transformMeta.getTransform() ).thenReturn( new InsertUpdateMeta() );

    InsertUpdate insertUpdateTransform =
      new InsertUpdate( mockHelper.transformMeta,mockHelper.iTransformMeta, mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    insertUpdateTransform = Mockito.spy( insertUpdateTransform );

    Mockito.doReturn( new Object[] {} ).when( insertUpdateTransform ).getRow();
    insertUpdateTransform.first = false;
    mockHelper.iTransformData.lookupParameterRowMeta = Mockito.mock( IRowMeta.class );
    mockHelper.iTransformData.keynrs = new int[] {};
    mockHelper.iTransformData.db = Mockito.mock( Database.class );
    mockHelper.iTransformData.valuenrs = new int[] {};
    Mockito.doThrow( new HopTransformException( "Test exception" ) ).when( insertUpdateTransform ).putRow( Mockito.any(), Mockito.any() );

    boolean result =
      insertUpdateTransform.processRow();
    Assert.assertFalse( result );
  }

  //PDI-16349
  @Test
  @Ignore
  public void keyStream2ProcessRow() throws HopException {
    InsertUpdate insertUpdateTransform =
      new InsertUpdate( mockHelper.transformMeta, mockHelper.iTransformMeta,  mockHelper.iTransformData, 0, mockHelper.pipelineMeta, mockHelper.pipeline );
    insertUpdateTransform.setInputRowMeta( Mockito.mock( IRowMeta.class ) );
    insertUpdateTransform = Mockito.spy( insertUpdateTransform );

    InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
    insertUpdateMeta.setKeyStream( new String[] { "test_field" } );
    insertUpdateMeta.setKeyCondition( new String[] { "test_condition" } );
    insertUpdateMeta.setKeyStream2( new String[] {} );
    insertUpdateMeta.setUpdateLookup( new String[] {} );
    insertUpdateMeta.setKeyLookup( new String[] {} );
    insertUpdateMeta.setUpdateBypassed( true );
    insertUpdateMeta.setDatabaseMeta( Mockito.mock( DatabaseMeta.class ) );
    Database database = Mockito.mock( Database.class );
    mockHelper.iTransformData.db = database;
    Mockito.doReturn( Mockito.mock( Connection.class ) ).when( database ).getConnection();
    Mockito.doNothing().when( insertUpdateTransform ).lookupValues( Mockito.any(), Mockito.any() );
    Mockito.doNothing().when( insertUpdateTransform ).putRow( Mockito.any(), Mockito.any() );
    Mockito.doReturn( new Object[] {} ).when( insertUpdateTransform ).getRow();
    insertUpdateTransform.first = true;

    insertUpdateMeta.afterInjectionSynchronization();
    //run without a exception
    insertUpdateTransform.processRow();

    Assert.assertEquals( insertUpdateMeta.getKeyStream().length, insertUpdateMeta.getKeyStream2().length );
  }
}
