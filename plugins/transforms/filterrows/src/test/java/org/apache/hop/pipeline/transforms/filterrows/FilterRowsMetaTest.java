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
package org.apache.hop.pipeline.transforms.filterrows;

import com.google.common.collect.ImmutableList;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ConditionLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FilterRowsMetaTest {
  LoadSaveTester loadSaveTester;
  Class<FilterRowsMeta> testMetaClass = FilterRowsMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "condition", "send_true_to", "send_false_to" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "condition", new ConditionLoadSaveValidator() );
    attrValidatorMap.put( "trueTransformName", new StringLoadSaveValidator() );
    attrValidatorMap.put( "falseTransformName", new StringLoadSaveValidator() );

    getterMap.put( "send_true_to", "getTrueTransformName" );
    setterMap.put( "send_true_to", "setTrueTransformName" );
    getterMap.put( "send_false_to", "getFalseTransformName" );
    setterMap.put( "send_false_to", "setFalseTransformName" );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester = new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testClone() {
    FilterRowsMeta filterRowsMeta = new FilterRowsMeta();
    filterRowsMeta.setCondition( new Condition() );
    filterRowsMeta.setTrueTransformName( "true" );
    filterRowsMeta.setFalseTransformName( "false" );

    FilterRowsMeta clone = (FilterRowsMeta) filterRowsMeta.clone();
    assertNotNull( clone.getCondition() );
    assertEquals( "true", clone.getTrueTransformName() );
    assertEquals( "false", clone.getFalseTransformName() );
  }

  @Test
  public void modifiedTarget() throws Exception {
    FilterRowsMeta filterRowsMeta = new FilterRowsMeta();
    TransformMeta trueOutput = new TransformMeta( "true", new DummyMeta() );
    TransformMeta falseOutput = new TransformMeta( "false", new DummyMeta() );

    filterRowsMeta.setCondition( new Condition() );
    filterRowsMeta.setTrueTransformName( trueOutput.getName() );
    filterRowsMeta.setFalseTransformName( falseOutput.getName() );
    filterRowsMeta.searchInfoAndTargetTransforms( ImmutableList.of( trueOutput, falseOutput ) );

    trueOutput.setName( "true renamed" );
    falseOutput.setName( "false renamed" );

    assertEquals( "true renamed", filterRowsMeta.getTrueTransformName() );
    assertEquals( "false renamed", filterRowsMeta.getFalseTransformName() );
  }
}
