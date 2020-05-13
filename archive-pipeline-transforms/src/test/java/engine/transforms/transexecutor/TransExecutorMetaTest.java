/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineExecutorMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( false );
  }

  @Before
  public void setUp() throws Exception {

    List<String> attributes =
      Arrays.asList( "fileName", "groupSize", "groupField", "groupTime",
        "executionTimeField", "executionFilesRetrievedField", "executionLogTextField",
        "executionLogChannelIdField", "executionResultField", "executionNrErrorsField", "executionLinesReadField",
        "executionLinesWrittenField", "executionLinesInputField", "executionLinesOutputField",
        "executionLinesRejectedField", "executionLinesUpdatedField", "executionLinesDeletedField",
        "executionExitStatusField", "outputRowsField", "outputRowsType", "outputRowsLength",
        "outputRowsPrecision" );

    // executionResultTargetTransformMeta -? (see for switch case meta)
    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 1 );

    // don't want the random value given by the default getTestObject...
    IntLoadSaveValidator intValidator = spy( new IntLoadSaveValidator() );
    doReturn( IValueMeta.TYPE_INTEGER ).when( intValidator ).getTestObject();

    FieldLoadSaveValidator<int[]> intArrayLoadSaveValidator =
      new PrimitiveIntArrayLoadSaveValidator( intValidator, 1 );

    attrValidatorMap.put( "outputRowsField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "outputRowsType", intArrayLoadSaveValidator );
    attrValidatorMap.put( "outputRowsLength", intArrayLoadSaveValidator );
    attrValidatorMap.put( "outputRowsPrecision", intArrayLoadSaveValidator );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    typeValidatorMap.put( int[].class.getCanonicalName(), new PrimitiveIntArrayLoadSaveValidator(
      new IntLoadSaveValidator(), 1 ) );
    loadSaveTester =
      new LoadSaveTester( PipelineExecutorMeta.class, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );

  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }


  @Test
  public void firstStreamIsExecutionStatistics() throws Exception {
    StreamInterface stream = mockStream();
    TransformIOMetaInterface transformIo = mockTransformIo( stream, 0 );

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    when( meta.getTransformIOMeta() ).thenReturn( transformIo );
    doCallRealMethod().when( meta ).handleStreamSelection( any( StreamInterface.class ) );

    meta.handleStreamSelection( stream );

    assertEquals( stream.getTransformMeta(), meta.getExecutionResultTargetTransformMeta() );
  }

  @Test
  public void secondStreamIsInternalPipelinesOutput() throws Exception {
    StreamInterface stream = mockStream();
    TransformIOMetaInterface transformIo = mockTransformIo( stream, 1 );

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    when( meta.getTransformIOMeta() ).thenReturn( transformIo );
    doCallRealMethod().when( meta ).handleStreamSelection( any( StreamInterface.class ) );

    meta.handleStreamSelection( stream );

    assertEquals( stream.getTransformMeta(), meta.getOutputRowsSourceTransformMeta() );
  }

  @Test
  public void thirdStreamIsExecutionResultFiles() throws Exception {
    StreamInterface stream = mockStream();
    TransformIOMetaInterface transformIo = mockTransformIo( stream, 2 );

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    when( meta.getTransformIOMeta() ).thenReturn( transformIo );
    doCallRealMethod().when( meta ).handleStreamSelection( any( StreamInterface.class ) );

    meta.handleStreamSelection( stream );

    assertEquals( stream.getTransformMeta(), meta.getResultFilesTargetTransformMeta() );
  }

  @Test
  public void forthStreamIsExecutorsInput() throws Exception {
    StreamInterface stream = mockStream();
    TransformIOMetaInterface transformIo = mockTransformIo( stream, 3 );

    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    when( meta.getTransformIOMeta() ).thenReturn( transformIo );
    doCallRealMethod().when( meta ).handleStreamSelection( any( StreamInterface.class ) );

    meta.handleStreamSelection( stream );

    assertEquals( stream.getTransformMeta(), meta.getExecutorsOutputTransformMeta() );
  }

  @Test
  public void testPrepareExecutionResultsFields() throws Exception {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    IRowMeta row = mock( IRowMeta.class );
    TransformMeta nextTransform = mock( TransformMeta.class );

    meta.setExecutionResultTargetTransformMeta( nextTransform );
    meta.setExecutionTimeField( "time" );

    TransformMeta parent = mock( TransformMeta.class );
    doReturn( parent ).when( meta ).getParentTransformMeta();
    when( parent.getName() ).thenReturn( "parent transform" );

    meta.prepareExecutionResultsFields( row, nextTransform );

    // make sure we get the name of the parent transform meta... used for the origin transform
    verify( parent ).getName();
    ArgumentCaptor<IValueMeta> argumentCaptor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( row ).addValueMeta( argumentCaptor.capture() );
    assertEquals( "parent transform", argumentCaptor.getValue().getOrigin() );
  }

  @Test
  public void testPrepareExecutionResultsFileFields() throws Exception {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    IRowMeta row = mock( IRowMeta.class );
    TransformMeta nextTransform = mock( TransformMeta.class );

    meta.setResultFilesTargetTransformMeta( nextTransform );
    meta.setResultFilesFileNameField( "file_name" );

    TransformMeta parent = mock( TransformMeta.class );
    doReturn( parent ).when( meta ).getParentTransformMeta();
    when( parent.getName() ).thenReturn( "parent transform" );

    meta.prepareExecutionResultsFileFields( row, nextTransform );

    // make sure we get the name of the parent transform meta... used for the origin transform
    verify( parent ).getName();
    ArgumentCaptor<IValueMeta> argumentCaptor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( row ).addValueMeta( argumentCaptor.capture() );
    assertEquals( "parent transform", argumentCaptor.getValue().getOrigin() );
  }

  @Test
  public void testPrepareResultsRowsFields() throws Exception {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    String[] outputFieldNames = new String[] { "one", "two" };
    int[] outputFieldTypes = new int[] { 0, 1 };
    int[] outputFieldLength = new int[] { 4, 8 };
    int[] outputFieldPrecision = new int[] { 2, 4 };

    meta.setOutputRowsField( outputFieldNames );
    meta.setOutputRowsType( outputFieldTypes );
    meta.setOutputRowsLength( outputFieldLength );
    meta.setOutputRowsPrecision( outputFieldPrecision );
    meta = spy( meta );

    IRowMeta row = mock( IRowMeta.class );

    TransformMeta parent = mock( TransformMeta.class );
    doReturn( parent ).when( meta ).getParentTransformMeta();
    when( parent.getName() ).thenReturn( "parent transform" );

    meta.prepareResultsRowsFields( row );

    // make sure we get the name of the parent transform meta... used for the origin transform
    verify( parent, times( outputFieldNames.length ) ).getName();
    ArgumentCaptor<IValueMeta> argumentCaptor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( row, times( outputFieldNames.length ) ).addValueMeta( argumentCaptor.capture() );
    assertEquals( "parent transform", argumentCaptor.getValue().getOrigin() );
  }

  @Test
  public void testGetFields() throws Exception {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta = spy( meta );

    TransformMeta nextTransform = mock( TransformMeta.class );

    // Test null
    meta.getFields( null, null, null, nextTransform, null, null );
    verify( meta, never() ).addFieldToRow( any( IRowMeta.class ), anyString(), anyInt() );

    IRowMeta rowMeta = mock( IRowMeta.class );
    meta.getFields( rowMeta, null, null, nextTransform, null, null );
    verify( rowMeta, never() ).clear();

    TransformMeta executionResultTargetTransformMeta = mock( TransformMeta.class );
    meta.setExecutionResultTargetTransformMeta( executionResultTargetTransformMeta );
    meta.getFields( rowMeta, null, null, nextTransform, null, null );
    verify( rowMeta, atMost( 1 ) ).clear();
    meta.setExecutionResultTargetTransformMeta( null );

    TransformMeta resultFilesTargetTransformMeta = mock( TransformMeta.class );
    meta.setResultFilesTargetTransformMeta( resultFilesTargetTransformMeta );
    meta.getFields( rowMeta, null, null, nextTransform, null, null );
    verify( rowMeta, atMost( 1 ) ).clear();
    meta.setResultFilesTargetTransformMeta( null );

    TransformMeta outputRowsSourceTransformMeta = mock( TransformMeta.class );
    meta.setOutputRowsSourceTransformMeta( outputRowsSourceTransformMeta );
    meta.getFields( rowMeta, null, null, nextTransform, null, null );
    verify( rowMeta, atMost( 1 ) ).clear();
    meta.setOutputRowsSourceTransformMeta( null );
  }

  @Test
  public void testClone() throws Exception {
    PipelineExecutorMeta meta = new PipelineExecutorMeta();
    meta.setOutputRowsField( new String[] { "field1", "field2" } );
    meta.setOutputRowsLength( new int[] { 5, 5 } );
    meta.setOutputRowsPrecision( new int[] { 5, 5 } );
    meta.setOutputRowsType( new int[] { 0, 0 } );

    PipelineExecutorMeta cloned = (PipelineExecutorMeta) meta.clone();
    assertFalse( cloned.getOutputRowsField() == meta.getOutputRowsField() );
    assertTrue( Arrays.equals( cloned.getOutputRowsField(), meta.getOutputRowsField() ) );
    assertFalse( cloned.getOutputRowsLength() == meta.getOutputRowsLength() );
    assertTrue( Arrays.equals( cloned.getOutputRowsLength(), meta.getOutputRowsLength() ) );
    assertFalse( cloned.getOutputRowsPrecision() == meta.getOutputRowsPrecision() );
    assertTrue( Arrays.equals( cloned.getOutputRowsPrecision(), meta.getOutputRowsPrecision() ) );
    assertFalse( cloned.getOutputRowsType() == meta.getOutputRowsType() );
    assertTrue( Arrays.equals( cloned.getOutputRowsType(), meta.getOutputRowsType() ) );
  }

  @SuppressWarnings( "unchecked" )
  private static TransformIOMetaInterface mockTransformIo( StreamInterface stream, int desiredIndex ) {
    List<StreamInterface> list = mock( List.class );
    when( list.indexOf( stream ) ).thenReturn( desiredIndex );
    when( list.get( eq( desiredIndex ) ) ).thenReturn( stream );

    TransformIOMetaInterface transformIo = mock( TransformIOMetaInterface.class );
    when( transformIo.getTargetStreams() ).thenReturn( list );
    return transformIo;
  }

  private static StreamInterface mockStream() {
    TransformMeta transformMeta = mock( TransformMeta.class );
    StreamInterface stream = mock( StreamInterface.class );
    when( stream.getTransformMeta() ).thenReturn( transformMeta );
    return stream;
  }


  @Test
  public void testRemoveHopFrom() throws Exception {
    PipelineExecutorMeta pipelineExecutorMeta = new PipelineExecutorMeta();
    pipelineExecutorMeta.setExecutionResultTargetTransformMeta( new TransformMeta() );
    pipelineExecutorMeta.setOutputRowsSourceTransformMeta( new TransformMeta() );
    pipelineExecutorMeta.setResultFilesTargetTransformMeta( new TransformMeta() );
    pipelineExecutorMeta.setExecutorsOutputTransformMeta( new TransformMeta() );

    pipelineExecutorMeta.cleanAfterHopFromRemove();

    assertNull( pipelineExecutorMeta.getExecutionResultTargetTransformMeta() );
    assertNull( pipelineExecutorMeta.getOutputRowsSourceTransformMeta() );
    assertNull( pipelineExecutorMeta.getResultFilesTargetTransformMeta() );
    assertNull( pipelineExecutorMeta.getExecutorsOutputTransformMeta() );
  }

}
