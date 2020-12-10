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
package org.apache.hop.pipeline.transforms.joinrows;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ConditionLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class JoinRowsMetaTest {
  LoadSaveTester loadSaveTester;
  Class<JoinRowsMeta> testMetaClass = JoinRowsMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "directory", "prefix", "cacheSize", "mainTransformName", "condition" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "condition", new ConditionLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testCleanAfterHopToRemove_NullParameter() {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();
    TransformMeta transformMeta1 = new TransformMeta( "Transform1", mock( ITransformMeta.class ) );
    joinRowsMeta.setMainTransform( transformMeta1 );
    joinRowsMeta.setMainTransformName( transformMeta1.getName() );

    // This call must not throw an exception
    joinRowsMeta.cleanAfterHopToRemove( null );

    // And no change to the transform should be made
    assertEquals( transformMeta1, joinRowsMeta.getMainTransform() );
    assertEquals( transformMeta1.getName(), joinRowsMeta.getMainTransformName() );
  }

  @Test
  public void testCleanAfterHopToRemove_UnknownTransform() {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();

    TransformMeta transformMeta1 = new TransformMeta( "Transform1", mock( ITransformMeta.class ) );
    TransformMeta transformMeta2 = new TransformMeta( "Transform2", mock( ITransformMeta.class ) );
    joinRowsMeta.setMainTransform( transformMeta1 );
    joinRowsMeta.setMainTransformName( transformMeta1.getName() );

    joinRowsMeta.cleanAfterHopToRemove( transformMeta2 );

    // No change to the transform should be made
    assertEquals( transformMeta1, joinRowsMeta.getMainTransform() );
    assertEquals( transformMeta1.getName(), joinRowsMeta.getMainTransformName() );
  }

  @Test
  public void testCleanAfterHopToRemove_ReferredTransform() {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();

    TransformMeta transformMeta1 = new TransformMeta( "Transform1", mock( ITransformMeta.class ) );
    joinRowsMeta.setMainTransform( transformMeta1 );
    joinRowsMeta.setMainTransformName( transformMeta1.getName() );

    joinRowsMeta.cleanAfterHopToRemove( transformMeta1 );

    // No change to the transform should be made
    assertNull( joinRowsMeta.getMainTransform() );
    assertNull( joinRowsMeta.getMainTransformName() );
  }
}
