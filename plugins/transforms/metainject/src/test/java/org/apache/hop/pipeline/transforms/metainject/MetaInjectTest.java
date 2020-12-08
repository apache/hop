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


package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;

@RunWith( PowerMockRunner.class )
@PrepareForTest( { MetaInject.class } )
public class MetaInjectTest {

  private static final String INJECTOR_TRANSFORM_NAME = "TEST_TRANSFORM_FOR_INJECTION";

  private static final String TEST_VALUE = "TEST_VALUE";

  private static final String TEST_VARIABLE = "TEST_VARIABLE";

  private static final String TEST_PARAMETER = "TEST_PARAMETER";

  private static final String TEST_TARGET_TRANSFORM_NAME = "TEST_TARGET_TRANSFORM_NAME";

  private static final String TEST_SOURCE_TRANSFORM_NAME = "TEST_SOURCE_TRANSFORM_NAME";

  private static final String TEST_ATTR_VALUE = "TEST_ATTR_VALUE";

  private static final String TEST_FIELD = "TEST_FIELD";

  private static final String UNAVAILABLE_TRANSFORM = "UNAVAILABLE_TRANSFORM";

  private static final TargetTransformAttribute UNAVAILABLE_TARGET_TRANSFORM =
      new TargetTransformAttribute(UNAVAILABLE_TRANSFORM, TEST_ATTR_VALUE, false );

  private static final SourceTransformField UNAVAILABLE_SOURCE_TRANSFORM = new SourceTransformField(UNAVAILABLE_TRANSFORM, TEST_FIELD );

  private MetaInject metaInject;

  private MetaInjectMeta meta;

  private MetaInjectData data;

  private PipelineMeta pipelineMeta;

  private Pipeline pipeline;

  private IHopMetadataProvider metadataProvider;


  @Before
  public void before() throws Exception {
    pipelineMeta = PowerMockito.spy( new PipelineMeta() );
    meta = new MetaInjectMeta();
    data = new MetaInjectData();
    data.pipelineMeta = pipelineMeta;
    metaInject = TransformMockUtil.getTransform( MetaInject.class, MetaInjectMeta.class, MetaInjectData.class, "MetaInjectTest" );
    metaInject = PowerMockito.spy( metaInject );
    metaInject.init();
    metadataProvider = mock( IHopMetadataProvider.class );
    metaInject.setMetadataProvider( metadataProvider );
    doReturn( pipelineMeta ).when( metaInject ).getPipelineMeta();

    PipelineMeta internalPipelineMeta = mock( PipelineMeta.class );
    TransformMeta transformMeta = mock( TransformMeta.class );
    pipeline = new LocalPipelineEngine();
    pipeline.setLogChannel( LogChannel.GENERAL );
    pipeline = PowerMockito.spy(pipeline);
    doReturn(pipeline).when( metaInject ).getPipeline();
    doReturn( INJECTOR_TRANSFORM_NAME ).when( transformMeta ).getName();
    doReturn( Collections.singletonList( transformMeta ) ).when( internalPipelineMeta ).getUsedTransforms();
    ITransformMeta iTransformMeta = mock( ITransformMeta.class );
    doReturn( iTransformMeta ).when( transformMeta ).getTransform();
    doReturn( internalPipelineMeta ).when( metaInject ).loadPipelineMeta();
  }

  @Test
  public void getUnavailableSourceTransforms() {
    TargetTransformAttribute targetTransform = new TargetTransformAttribute(TEST_TARGET_TRANSFORM_NAME, TEST_ATTR_VALUE, false );
    SourceTransformField unavailableSourceTransform = new SourceTransformField(UNAVAILABLE_TRANSFORM, TEST_FIELD );
    Map<TargetTransformAttribute, SourceTransformField> targetMap = Collections.singletonMap( targetTransform, unavailableSourceTransform );
    PipelineMeta sourcePipelineMeta = mock( PipelineMeta.class );
    doReturn( new String[0] ).when( sourcePipelineMeta ).getPrevTransformNames( any( TransformMeta.class ) );

    Set<SourceTransformField> actualSet =
        MetaInject.getUnavailableSourceTransforms( targetMap, sourcePipelineMeta, mock( TransformMeta.class ) );
    assertTrue( actualSet.contains( unavailableSourceTransform ) );
  }

  @Test
  public void getUnavailableTargetTransforms() {
    TargetTransformAttribute unavailableTargetTransform = new TargetTransformAttribute(UNAVAILABLE_TRANSFORM, TEST_ATTR_VALUE, false );
    SourceTransformField sourceTransform = new SourceTransformField(TEST_SOURCE_TRANSFORM_NAME, TEST_FIELD );
    Map<TargetTransformAttribute, SourceTransformField> targetMap = Collections.singletonMap( unavailableTargetTransform, sourceTransform );
    PipelineMeta injectedPipelineMeta = mock( PipelineMeta.class );
    doReturn( Collections.emptyList() ).when( injectedPipelineMeta ).getUsedTransforms();

    Set<TargetTransformAttribute> actualSet = MetaInject.getUnavailableTargetTransforms( targetMap, injectedPipelineMeta );
    assertTrue( actualSet.contains( unavailableTargetTransform ) );
  }

  @Test
  public void removeUnavailableTransformsFromMapping_unavailable_source_transform() {
    TargetTransformAttribute unavailableTargetTransform = new TargetTransformAttribute(UNAVAILABLE_TRANSFORM, TEST_ATTR_VALUE, false );
    SourceTransformField unavailableSourceTransform = new SourceTransformField(UNAVAILABLE_TRANSFORM, TEST_FIELD );
    Map<TargetTransformAttribute, SourceTransformField> targetMap = new HashMap<>();
    targetMap.put( unavailableTargetTransform, unavailableSourceTransform );

    Set<SourceTransformField> unavailableSourceTransforms = Collections.singleton(UNAVAILABLE_SOURCE_TRANSFORM);
    MetaInject.removeUnavailableTransformsFromMapping( targetMap, unavailableSourceTransforms, Collections
        .emptySet() );
    assertTrue( targetMap.isEmpty() );
  }

  @Test
  public void removeUnavailableTransformsFromMapping_unavailable_target_transform() {
    TargetTransformAttribute unavailableTargetTransform = new TargetTransformAttribute(UNAVAILABLE_TRANSFORM, TEST_ATTR_VALUE, false );
    SourceTransformField unavailableSourceTransform = new SourceTransformField(UNAVAILABLE_TRANSFORM, TEST_FIELD );
    Map<TargetTransformAttribute, SourceTransformField> targetMap = new HashMap<>();
    targetMap.put( unavailableTargetTransform, unavailableSourceTransform );

    Set<TargetTransformAttribute> unavailableTargetTransforms = Collections.singleton(UNAVAILABLE_TARGET_TRANSFORM);
    MetaInject.removeUnavailableTransformsFromMapping( targetMap, Collections.emptySet(),
        unavailableTargetTransforms );
    assertTrue( targetMap.isEmpty() );
  }

  @Test
  public void removeUnavailableTransformsFromMapping_unavailable_source_target_transform() {
    TargetTransformAttribute unavailableTargetTransform = new TargetTransformAttribute(UNAVAILABLE_TRANSFORM, TEST_ATTR_VALUE, false );
    SourceTransformField unavailableSourceTransform = new SourceTransformField(UNAVAILABLE_TRANSFORM, TEST_FIELD );
    Map<TargetTransformAttribute, SourceTransformField> targetMap = new HashMap<>();
    targetMap.put( unavailableTargetTransform, unavailableSourceTransform );

    Set<TargetTransformAttribute> unavailableTargetTransforms = Collections.singleton(UNAVAILABLE_TARGET_TRANSFORM);
    Set<SourceTransformField> unavailableSourceTransforms = Collections.singleton(UNAVAILABLE_SOURCE_TRANSFORM);
    MetaInject.removeUnavailableTransformsFromMapping( targetMap, unavailableSourceTransforms, unavailableTargetTransforms );
    assertTrue( targetMap.isEmpty() );
  }

  @Test
  public void convertToUpperCaseSet_null_array() {
    Set<String> actualResult = MetaInject.convertToUpperCaseSet( null );
    assertNotNull( actualResult );
    assertTrue( actualResult.isEmpty() );
  }

  @Test
  public void convertToUpperCaseSet() {
    String[] input = new String[] { "Test_Transform", "test_transform1" };
    Set<String> actualResult = MetaInject.convertToUpperCaseSet( input );
    Set<String> expectedResult = new HashSet<>();
    expectedResult.add( "TEST_TRANSFORM" );
    expectedResult.add( "TEST_TRANSFORM1" );
    assertEquals( expectedResult, actualResult );
  }

  @Test
  public void testGetUnavailableTargetKeys() throws Exception {
    final String targetTransformName = "injectable transform name";
    TargetTransformAttribute unavailableTargetAttr = new TargetTransformAttribute( targetTransformName, "NOT_THERE", false );
    TargetTransformAttribute availableTargetAttr = new TargetTransformAttribute( targetTransformName, "THERE", false );
    SourceTransformField sourceTransform = new SourceTransformField(TEST_SOURCE_TRANSFORM_NAME, TEST_FIELD );

    Map<TargetTransformAttribute, SourceTransformField> targetMap = new HashMap<>( 2 );
    targetMap.put( unavailableTargetAttr, sourceTransform );
    targetMap.put( availableTargetAttr, sourceTransform );

    ITransformMeta smi = new InjectableTestTransformMeta();
    PipelineMeta pipelineMeta = mockSingleTransformPipelineMeta( targetTransformName, smi );
    Set<TargetTransformAttribute> unavailable =
        MetaInject.getUnavailableTargetKeys( targetMap, pipelineMeta, Collections.emptySet() );
    assertEquals( 1, unavailable.size() );
    assertTrue( unavailable.contains( unavailableTargetAttr ) );
  }
  

  @Test
  public void testTransformChangeListener() throws Exception {
    MetaInjectMeta mim = new MetaInjectMeta();
    TransformMeta sm = new TransformMeta( "testTransform", mim );
    try {
      pipelineMeta.addOrReplaceTransform( sm );
    } catch ( Exception ex ) {
      fail();
    }
  }

  private PipelineMeta mockSingleTransformPipelineMeta(final String targetTransformName, ITransformMeta smi ) {
    TransformMeta transformMeta = mock( TransformMeta.class );
    when( transformMeta.getTransform() ).thenReturn( smi );
    when( transformMeta.getName() ).thenReturn( targetTransformName );
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    when( pipelineMeta.getUsedTransforms() ).thenReturn( Collections.singletonList( transformMeta ) );
    return pipelineMeta;
  }

  @InjectionSupported( localizationPrefix = "", groups = "groups" )
  private static class InjectableTestTransformMeta extends BaseTransformMeta implements ITransformMeta<MetaInject, MetaInjectData> {

    @Injection( name = "THERE" )
    private String there;

    @Override
    public void setDefault() {
    }

    @Override
    public ITransform createTransform(TransformMeta transformMeta, MetaInjectData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
      return new MetaInject(transformMeta, new MetaInjectMeta(), data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public MetaInjectData getTransformData() {
      return null;
    }
  }


}
