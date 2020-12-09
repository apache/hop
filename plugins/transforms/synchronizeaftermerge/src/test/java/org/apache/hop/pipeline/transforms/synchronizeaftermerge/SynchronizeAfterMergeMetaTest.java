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
package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class SynchronizeAfterMergeMetaTest implements IInitializer<ITransform> {
  LoadSaveTester loadSaveTester;
  Class<SynchronizeAfterMergeMeta> testMetaClass = SynchronizeAfterMergeMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "schemaName", "tableName", "databaseMeta", "commitSize", "tableNameInField", "tablenameField",
        "operationOrderField", "useBatchUpdate", "performLookup", "OrderInsert", "OrderUpdate", "OrderDelete",
        "keyStream", "keyLookup", "keyCondition", "keyStream2", "updateLookup", "updateStream", "update" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "tableNameInField", "istablenameInField" );
        put( "tablenameField", "gettablenameField" );
        put( "useBatchUpdate", "useBatchUpdate" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "tableNameInField", "settablenameInField" );
        put( "tablenameField", "settablenameField" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );


    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "keyStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyStream2", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyCondition", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "updateLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "updateStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "update", new ArrayLoadSaveValidator<>( new BooleanLoadSaveValidator(), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof SynchronizeAfterMergeMeta ) {
      ( (SynchronizeAfterMergeMeta) someMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testPDI16559() throws Exception {
    SynchronizeAfterMergeMeta synchronizeAfterMerge = new SynchronizeAfterMergeMeta();

    synchronizeAfterMerge.setKeyStream( new String[] { "field1", "field2", "field3", "field4", "field5" } );
    synchronizeAfterMerge.setKeyLookup( new String[] { "lookup1", "lookup2" } );
    synchronizeAfterMerge.setKeyCondition( new String[] { "cond1", "cond2", "cond3" } );
    synchronizeAfterMerge.setKeyStream2( new String[] { "stream2-a", "stream2-b", "stream2-x", "stream2-d" } );

    synchronizeAfterMerge.setUpdateLookup( new String[] { "updlook1", "updlook2", "updlook3", "updlook4", "updlook5" } );
    synchronizeAfterMerge.setUpdateStream( new String[] { "updstr1", "updstr2", "updstr3" } );
    synchronizeAfterMerge.setUpdate( new Boolean[] { false, true } );

    synchronizeAfterMerge.afterInjectionSynchronization();
    String ktrXml = synchronizeAfterMerge.getXml();

    int targetSz = synchronizeAfterMerge.getKeyStream().length;

    Assert.assertEquals( targetSz, synchronizeAfterMerge.getKeyLookup().length );
    Assert.assertEquals( targetSz, synchronizeAfterMerge.getKeyCondition().length );
    Assert.assertEquals( targetSz, synchronizeAfterMerge.getKeyStream2().length );

    targetSz = synchronizeAfterMerge.getUpdateLookup().length;
    Assert.assertEquals( targetSz, synchronizeAfterMerge.getUpdateStream().length );
    Assert.assertEquals( targetSz, synchronizeAfterMerge.getUpdate().length );

  }
}
