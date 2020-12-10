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

package org.apache.hop.pipeline.transforms.update;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class UpdateMetaTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private TransformMeta transformMeta;
  private Update upd;
  private UpdateData ud;
  private UpdateMeta umi;
  LoadSaveTester loadSaveTester;
  Class<UpdateMeta> testMetaClass = UpdateMeta.class;
  private TransformMockHelper<UpdateMeta, UpdateData> mockHelper;

  @Before
  public void setUp() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init( false );
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "delete1" );

    Map<String, String> vars = new HashMap<>();
    vars.put( "max.sz", "10" );

    umi = new UpdateMeta();
    ud = new UpdateData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId( TransformPluginType.class, umi );

    transformMeta = new TransformMeta( deletePid, "delete", umi );
    Pipeline pipeline = new LocalPipelineEngine( pipelineMeta );
    pipeline.setVariables( vars );
    pipelineMeta.addTransform( transformMeta );
    mockHelper = new TransformMockHelper<>( "Update", UpdateMeta.class, UpdateData.class );
    Mockito.when( mockHelper.logChannelFactory.create( Mockito.any(), Mockito.any( ILoggingObject.class ) ) ).thenReturn( mockHelper.iLogChannel );

    upd = new Update( transformMeta, umi, ud, 1, pipelineMeta, pipeline );

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

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
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
  public void modify( ITransformMeta someMeta ) {
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
      String badXml = update.getXml();
      fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    update.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = update.getXml();

    int targetSz = update.getKeyStream().length;

    assertEquals( targetSz, update.getKeyLookup().length );
    assertEquals( targetSz, update.getKeyCondition().length );
    assertEquals( targetSz, update.getKeyStream2().length );

    targetSz = update.getUpdateLookup().length;
    assertEquals( targetSz, update.getUpdateStream().length );

  }

}
