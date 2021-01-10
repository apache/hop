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
package org.apache.hop.pipeline.transforms.multimerge;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Tatsiana_Kasiankova
 */
public class MultiMergeJoinMetaTest implements IInitializer<ITransform> {
  LoadSaveTester loadSaveTester;
  Class<MultiMergeJoinMeta> testMetaClass = MultiMergeJoinMeta.class;
  private MultiMergeJoinMeta multiMergeMeta;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    multiMergeMeta = new MultiMergeJoinMeta();
    List<String> attributes =
      Arrays.asList( "joinType", "keyFields", "inputTransforms" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );


    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, IFieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "keyFields", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "inputTransforms", stringArrayLoadSaveValidator );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, IFieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof MultiMergeJoinMeta ) {
      ( (MultiMergeJoinMeta) someMeta ).allocateKeys( 5 );
      ( (MultiMergeJoinMeta) someMeta ).allocateInputTransforms( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testSetGetInputTransforms() {
    assertNull( multiMergeMeta.getInputTransforms() );
    String[] inputTransforms = new String[] { "Transform1", "Transform2" };
    multiMergeMeta.setInputTransforms( inputTransforms );
    assertArrayEquals( inputTransforms, multiMergeMeta.getInputTransforms() );
  }

  @Test
  public void testGetXml() {
    String[] inputTransforms = new String[] { "Transform1", "Transform2" };
    multiMergeMeta.setInputTransforms( inputTransforms );
    multiMergeMeta.setKeyFields( new String[] { "Key1", "Key2" } );
    String xml = multiMergeMeta.getXml();
    Assert.assertTrue( xml.contains( "transform0" ) );
    Assert.assertTrue( xml.contains( "transform1" ) );

  }

  @Test
  public void cloneTest() throws Exception {
    MultiMergeJoinMeta meta = new MultiMergeJoinMeta();
    meta.allocateKeys( 2 );
    meta.allocateInputTransforms( 3 );
    meta.setKeyFields( new String[] { "key1", "key2" } );
    meta.setInputTransforms( new String[] { "transform1", "transform2", "transform3" } );
    // scalars should be cloned using super.clone() - makes sure they're calling super.clone()
    meta.setJoinType( "INNER" );
    MultiMergeJoinMeta aClone = (MultiMergeJoinMeta) meta.clone();
    Assert.assertFalse( aClone == meta );
    Assert.assertTrue( Arrays.equals( meta.getKeyFields(), aClone.getKeyFields() ) );
    Assert.assertTrue( Arrays.equals( meta.getInputTransforms(), aClone.getInputTransforms() ) );
    Assert.assertEquals( meta.getJoinType(), aClone.getJoinType() );
  }
}
