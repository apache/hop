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
package org.apache.hop.pipeline.transforms.streamlookup;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamLookupMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<StreamLookupMeta> testMetaClass = StreamLookupMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "inputSorted", "memoryPreservationActive", "usingSortedList", "usingIntegerPair", "keystream",
        "keylookup", "value", "valueName", "valueDefault", "valueDefaultType" );

    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "keystream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keylookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "value", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "valueName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "valueDefault", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "valueDefaultType", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 7 ), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        new HashMap<>(), new HashMap<>(), attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof StreamLookupMeta ) {
      ( (StreamLookupMeta) someMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testCloneInfoTransforms() {
    StreamLookupMeta meta = new StreamLookupMeta();
    meta.setDefault();

    final String transformName = UUID.randomUUID().toString();
    TransformMeta infoTransform = mock( TransformMeta.class );
    when( infoTransform.getName() ).thenReturn( transformName );
    meta.getTransformIOMeta().getInfoStreams().get( 0 ).setTransformMeta( infoTransform );

    StreamLookupMeta cloned = (StreamLookupMeta) meta.clone();
    assertEquals( transformName, cloned.getTransformIOMeta().getInfoStreams().get( 0 ).getTransformName() );
    assertNotSame( meta.getTransformIOMeta().getInfoStreams().get( 0 ),
      cloned.getTransformIOMeta().getInfoStreams().get( 0 ) );
  }

  //PDI-16110
  @Test
  public void testGetXml() {
    StreamLookupMeta streamLookupMeta = new StreamLookupMeta();
    streamLookupMeta.setKeystream( new String[] { "testKeyStreamValue" } );
    streamLookupMeta.setKeylookup( new String[] { "testKeyLookupValue" } );
    streamLookupMeta.setValue( new String[] { "testValue" } );
    streamLookupMeta.setValueName( new String[] {} );
    streamLookupMeta.setValueDefault( new String[] {} );
    streamLookupMeta.setValueDefaultType( new int[] {} );

    //run without exception
    streamLookupMeta.afterInjectionSynchronization();
    streamLookupMeta.getXml();

    Assert.assertEquals( streamLookupMeta.getKeystream().length, streamLookupMeta.getValueName().length );
    Assert.assertEquals( streamLookupMeta.getKeystream().length, streamLookupMeta.getValueDefault().length );
    Assert.assertEquals( streamLookupMeta.getKeystream().length, streamLookupMeta.getValueDefaultType().length );
  }

}
