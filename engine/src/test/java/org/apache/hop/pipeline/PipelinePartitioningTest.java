/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopPipelineException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <p>This test verify pipeline transform initializations and row sets distributions based
 * on different transforms execution. In this tests uses one transform as a producer and one transform as a consumer.
 * Examines different situations when transform runs in multiple copies or partitioned. One of
 * the possible issues of incorrect rowsets initialization described in PDI-12140.</p>
 * So next combinations is examined:
 * <ol>
 * <li>1 - 2x - when one transform copy is hoped to transform running in 2 copies
 * <li>2x - 2x - when transform running in 2 copies hops to transform running in 2 copies
 * <li>2x - 1 - when transform running in 2 copies hops to transform running in 1 copy
 * <li>1 - cl1 - when transform running in one copy hops to transform running partitioned
 * <li>cl1-cl1 - when transform running partitioned hops to transform running partitioned (swim lanes case)
 * <li>cl1-cl2 - when transform running partitioned by one partitioner hops to transform partitioned by another partitioner
 * <li>x2-cl1 - when transform running in 2 copies hops to partitioned transform
 */
public class PipelinePartitioningTest {

  /**
   * This is convenient names for testing transforms in pipeline.
   * <p>
   * The trick is if we use numeric names for transforms we can use NavigableSet to find next or previous when mocking
   * appropriate PipelineMeta methods (comparable strings).
   */
  private final String ONE = "1";
  private final String TWO = "2";
  private final String S10 = "1.0";
  private final String S11 = "1.1";
  private final String S20 = "2.0";
  private final String S21 = "2.1";
  private final String PID1 = "a";
  private final String PID2 = "b";
  private final String SP10 = "1.a";
  private final String SP11 = "1.b";
  private final String SP20 = "2.a";
  private final String SP21 = "2.b";

  @Mock
  ILogChannel log;

  Pipeline pipeline;

  /**
   * Transform meta is sorted according TransformMeta name so using numbers of transform names we can easy build transform chain mock.
   */
  private final NavigableSet<TransformMeta> chain = new TreeSet<TransformMeta>();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    pipeline = new Pipeline() {
      @Override
      public void calculateBatchIdAndDateRange() throws HopPipelineException {
        // avoid NPE if called
      }

      @Override
      public void beginProcessing() throws HopPipelineException {
        // avoid NPE if called
      }
    };
    // prepare PipelineMeta complete mock:
    PipelineMeta meta = Mockito.mock( PipelineMeta.class );
    Mockito.when( meta.getName() ).thenReturn( "junit meta" );
    Mockito.when( meta.getPipelineType() ).thenReturn( PipelineType.Normal );

    Mockito.when( meta.getPipelineHopTransforms( Mockito.anyBoolean() ) ).thenAnswer( new Answer<List<TransformMeta>>() {
      @Override
      public List<TransformMeta> answer( InvocationOnMock invocation ) throws Throwable {
        return ( new ArrayList<TransformMeta>( chain ) );
      }
    } );
    Mockito.when( meta.findNextTransforms( Mockito.any( TransformMeta.class ) ) ).then( new Answer<List<TransformMeta>>() {
      @Override
      public List<TransformMeta> answer( InvocationOnMock invocation ) throws Throwable {
        Object obj = invocation.getArguments()[ 0 ];
        TransformMeta findFor = TransformMeta.class.cast( obj );
        List<TransformMeta> ret = new ArrayList<TransformMeta>();
        TransformMeta nextTransform = chain.higher( findFor );
        if ( nextTransform != null ) {
          ret.add( nextTransform );
        }
        return ret;
      }
    } );
    Mockito.when( meta.findPreviousTransforms( Mockito.any( TransformMeta.class ), Mockito.anyBoolean() ) ).thenAnswer(
      new Answer<List<TransformMeta>>() {
        @Override
        public List<TransformMeta> answer( InvocationOnMock invocation ) throws Throwable {
          Object obj = invocation.getArguments()[ 0 ];
          TransformMeta findFor = TransformMeta.class.cast( obj );
          List<TransformMeta> ret = new ArrayList<TransformMeta>();
          TransformMeta prevTransform = chain.lower( findFor );
          if ( prevTransform != null ) {
            ret.add( prevTransform );
          }
          return ret;
        }
      } );
    Mockito.when( meta.findTransform( Mockito.anyString() ) ).thenAnswer( new Answer<TransformMeta>() {
      @Override
      public TransformMeta answer( InvocationOnMock invocation ) throws Throwable {
        Object obj = invocation.getArguments()[ 0 ];
        String findFor = String.class.cast( obj );
        for ( TransformMeta item : chain ) {
          if ( item.getName().equals( findFor ) ) {
            return item;
          }
        }
        return null;
      }
    } );

    pipeline.setLog( log );
    pipeline.setPipelineMeta( meta );
  }

  /**
   * This checks pipeline initialization when using one to many copies
   *
   * @throws HopException
   */
  @Test
  public void testOneToManyCopies() throws HopException {
    prepareTransformMetas_1_x2();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 2 rowsets finally", 2, rowsets.size() );
    assertEquals( "We have 3 transforms: one producer and 2 copies of consumer", 3, pipeline.getTransforms().size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne = getTransformByName( S10 );
    assertTrue( "1 transform have no input row sets", transformOne.getInputRowSets().isEmpty() );
    assertEquals( "1 transform have 2 output rowsets", 2, transformOne.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( S20 );
    Assert.assertEquals( "2.0 transform have 12 input row sets", 1, transformTwo0.getInputRowSets().size() );
    Assert.assertTrue( "2.0 transform have no output row sets", transformTwo0.getOutputRowSets().isEmpty() );

    ITransform transformTwo1 = getTransformByName( S21 );
    Assert.assertEquals( "2.1 transform have 1 input row sets", 1, transformTwo1.getInputRowSets().size() );
    Assert.assertTrue( "2.1 transform have no output row sets", transformTwo1.getOutputRowSets().isEmpty() );
  }

  /**
   * This checks pipeline initialization when using many to many copies.
   *
   * @throws HopException
   */
  @Test
  public void testManyToManyCopies() throws HopException {
    prepareTransformMetas_x2_x2();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 2 rowsets finally", 2, rowsets.size() );
    assertEquals( "We have 4 transforms: 2 copies of producer and 2 copies of consumer", 4, pipeline.getTransforms().size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne0 = getTransformByName( S10 );
    assertTrue( "1 transform have no input row sets", transformOne0.getInputRowSets().isEmpty() );
    assertEquals( "1 transform have 1 output rowsets", 1, transformOne0.getOutputRowSets().size() );

    ITransform transformOne1 = getTransformByName( S11 );
    assertTrue( "1 transform have no input row sets", transformOne1.getInputRowSets().isEmpty() );
    assertEquals( "1 transform have 1 output rowsets", 1, transformOne1.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( S20 );
    Assert.assertEquals( "2.0 transform have 1 input row sets", 1, transformTwo0.getInputRowSets().size() );
    Assert.assertTrue( "2.0 transform have no output row sets", transformTwo0.getOutputRowSets().isEmpty() );

    ITransform transformTwo1 = getTransformByName( S21 );
    Assert.assertEquals( "2.1 transform have 1 input row sets", 1, transformTwo1.getInputRowSets().size() );
    Assert.assertTrue( "2.1 transform have no output row sets", transformTwo1.getOutputRowSets().isEmpty() );
  }

  /**
   * This checks pipeline initialization when using many copies to one next transform
   *
   * @throws HopException
   */
  @Test
  public void testManyToOneCopies() throws HopException {
    prepareTransformMetas_x2_1();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 2 rowsets finally", 2, rowsets.size() );
    assertEquals( "We have 4 transforms: 2 copies of producer and 2 copies of consumer", 3, pipeline.getTransforms().size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne0 = getTransformByName( S10 );
    assertTrue( "1 transform have no input row sets", transformOne0.getInputRowSets().isEmpty() );
    assertEquals( "1 transform have 1 output rowsets", 1, transformOne0.getOutputRowSets().size() );

    ITransform transformOne1 = getTransformByName( S11 );
    assertTrue( "1 transform have no input row sets", transformOne1.getInputRowSets().isEmpty() );
    assertEquals( "1 transform have 1 output rowsets", 1, transformOne1.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( S20 );
    Assert.assertEquals( "2.0 transform have 2 input row sets", 2, transformTwo0.getInputRowSets().size() );
    Assert.assertTrue( "2.0 transform have no output row sets", transformTwo0.getOutputRowSets().isEmpty() );
  }

  /**
   * Test one to one partitioning transform pipeline organization.
   *
   * @throws HopException
   */
  @Test
  public void testOneToPartitioningSchema() throws HopException {
    prepareTransformMetas_1_cl1();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 2 rowsets finally", 2, rowsets.size() );
    assertEquals( "We have 3 transforms: 1 producer and 2 copies of consumer since it is partitioned", 3, pipeline.getTransforms()
      .size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne0 = getTransformByName( S10 );
    assertTrue( "1 transform have no input row sets", transformOne0.getInputRowSets().isEmpty() );
    assertEquals( "1 transform have 2 output rowsets", 2, transformOne0.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( SP20 );
    assertEquals( "2.0 transform have one input row sets", 1, transformTwo0.getInputRowSets().size() );
    assertTrue( "2.0 transform have no output rowsets", transformTwo0.getOutputRowSets().isEmpty() );

    ITransform transformTwo1 = getTransformByName( SP21 );
    Assert.assertEquals( "2.1 transform have 1 input row sets", 1, transformTwo1.getInputRowSets().size() );
    Assert.assertTrue( "2.1 transform have no output row sets", transformTwo1.getOutputRowSets().isEmpty() );
  }

  /**
   * Test 'Swim lines partitioning'
   *
   * @throws HopException
   */
  @Test
  public void testSwimLanesPartitioning() throws HopException {
    prepareTransformMetas_cl1_cl1();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 2 rowsets finally", 2, rowsets.size() );
    assertEquals( "We have 3 transforms: 1 producer and 2 copies of consumer since it is partitioned", 4, pipeline.getTransforms()
      .size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne0 = getTransformByName( SP10 );
    assertTrue( "1.0 transform have no input row sets", transformOne0.getInputRowSets().isEmpty() );
    assertEquals( "1.0 transform have 1 output rowsets", 1, transformOne0.getOutputRowSets().size() );

    ITransform transformOne1 = getTransformByName( SP11 );
    assertTrue( "1.1 transform have no input row sets", transformOne1.getInputRowSets().isEmpty() );
    assertEquals( "1.1 transform have 1 output rowsets", 1, transformOne1.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( SP20 );
    assertEquals( "2.0 transform have 2 input row sets", 1, transformTwo0.getInputRowSets().size() );
    assertTrue( "2.0 transform have no output rowsets", transformTwo0.getOutputRowSets().isEmpty() );

    ITransform transformTwo2 = getTransformByName( SP21 );
    assertTrue( "2.2 transform have no output row sets", transformTwo2.getOutputRowSets().isEmpty() );
    assertEquals( "2.2 transform have 2 output rowsets", 1, transformTwo2.getInputRowSets().size() );
  }

  /**
   * This is PDI-12140 case. 2 transforms with same partitions ID's count but different partitioner. This is not a swim lines
   * cases and we need repartitioning here.
   *
   * @throws HopException
   */
  @Test
  public void testDifferentPartitioningFlow() throws HopException {
    prepareTransformMetas_cl1_cl2();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 4 rowsets finally since repartitioning happens", 4, rowsets.size() );
    assertEquals( "We have 4 transforms: 2 producer copies and 2 copies of consumer since they both partitioned", 4, pipeline
      .getTransforms().size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne0 = getTransformByName( SP10 );
    assertTrue( "1.0 transform have no input row sets", transformOne0.getInputRowSets().isEmpty() );
    assertEquals( "1.0 transform have 2 output rowsets", 2, transformOne0.getOutputRowSets().size() );

    ITransform transformOne1 = getTransformByName( SP11 );
    assertTrue( "1.1 transform have no input row sets", transformOne1.getInputRowSets().isEmpty() );
    assertEquals( "1.1 transform have 2 output rowsets", 2, transformOne1.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( SP20 );
    assertTrue( "2.0 transform have no output row sets", transformTwo0.getOutputRowSets().isEmpty() );
    assertEquals( "2.0 transform have 1 input rowsets", 2, transformTwo0.getInputRowSets().size() );

    ITransform transformTwo2 = getTransformByName( SP21 );
    assertTrue( "2.1 transform have no output row sets", transformTwo2.getOutputRowSets().isEmpty() );
    assertEquals( "2.2 transform have 2 input rowsets", 2, transformTwo2.getInputRowSets().size() );
  }

  /**
   * This is a case when transform running in many copies meets partitioning one.
   *
   * @throws HopException
   */
  @Test
  public void testManyCopiesToPartitioningFlow() throws HopException {
    prepareTransformMetas_x2_cl1();

    pipeline.prepareExecution();
    List<IRowSet> rowsets = pipeline.getRowsets();
    assertTrue( !rowsets.isEmpty() );
    assertEquals( "We have 4 rowsets finally since repartitioning happens", 4, rowsets.size() );
    assertEquals( "We have 4 transforms: 2 producer copies and 2 copies of consumer since consumer is partitioned", 4, pipeline
      .getTransforms().size() );

    // Ok, examine initialized transforms now.
    ITransform transformOne0 = getTransformByName( S10 );
    assertTrue( "1.0 transform have no input row sets", transformOne0.getInputRowSets().isEmpty() );
    assertEquals( "1.0 transform have 2 output rowsets", 2, transformOne0.getOutputRowSets().size() );

    ITransform transformOne1 = getTransformByName( S11 );
    assertTrue( "1.1 transform have no input row sets", transformOne1.getInputRowSets().isEmpty() );
    assertEquals( "1.1 transform have 2 output rowsets", 2, transformOne1.getOutputRowSets().size() );

    ITransform transformTwo0 = getTransformByName( SP20 );
    assertTrue( "2.0 transform have no output row sets", transformTwo0.getOutputRowSets().isEmpty() );
    assertEquals( "2.0 transform have 2 input rowsets", 2, transformTwo0.getInputRowSets().size() );

    ITransform transformTwo2 = getTransformByName( SP21 );
    assertTrue( "2.1 transform have no output row sets", transformTwo2.getOutputRowSets().isEmpty() );
    assertEquals( "2.2 transform have 2 input rowsets", 2, transformTwo2.getInputRowSets().size() );
  }

  private ITransform getTransformByName( String name ) {
    List<TransformMetaDataCombi<ITransform, ITransformMeta, ITransformData>> combiList = pipeline.getTransforms();
    for ( TransformMetaDataCombi item : combiList ) {
      if ( item.transform.toString().equals( name ) ) {
        return item.transform;
      }
    }
    fail( "Test error, can't find transform with name: " + name );
    // and this will never happens.
    return null;
  }

  /**
   * one 'regular transform' to 'transform running in 2 copies'
   */
  private void prepareTransformMetas_1_x2() {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );
    dummy2.setCopies( 2 );
    chain.add( dummy1 );
    chain.add( dummy2 );

    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }

  /**
   * one 'transform running in 2 copies' to 'transform running in 2 copies'
   */
  private void prepareTransformMetas_x2_x2() {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );
    dummy1.setCopies( 2 );
    dummy2.setCopies( 2 );
    chain.add( dummy1 );
    chain.add( dummy2 );

    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }

  /**
   * many transforms copies to one
   */
  private void prepareTransformMetas_x2_1() {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );
    dummy1.setCopies( 2 );
    chain.add( dummy1 );
    chain.add( dummy2 );

    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }

  /**
   * This is a case when we have 1 transform to 1 clustered transform distribution.
   *
   * @throws HopPluginException
   */
  private void prepareTransformMetas_1_cl1() throws HopPluginException {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );

    PartitionSchema schema = new PartitionSchema( "p1", Arrays.asList( new String[] { PID1, PID2 } ) );
    TransformPartitioningMeta partMeta = new TransformPartitioningMeta( "Mirror to all partitions", schema );
    dummy2.setTransformPartitioningMeta( partMeta );

    chain.add( dummy1 );
    chain.add( dummy2 );
    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }

  /**
   * This case simulates when we do have 2 transform partitioned with one same partitioner We want to get a 'swim-lanes'
   * pipeline
   *
   * @throws HopPluginException
   */
  private void prepareTransformMetas_cl1_cl1() throws HopPluginException {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );

    PartitionSchema schema = new PartitionSchema( "p1", Arrays.asList( new String[] { PID1, PID2 } ) );
    // for delayed binding TransformPartitioning meta does not achieve
    // schema name when using in constructor so we have to set it
    // explicitly. See equals implementation for TransformPartitioningMeta.
    TransformPartitioningMeta partMeta = new TransformPartitioningMeta( "Mirror to all partitions", schema );
    // that is what I am talking about:
    partMeta.setPartitionSchema( schema );

    dummy1.setTransformPartitioningMeta( partMeta );
    dummy2.setTransformPartitioningMeta( partMeta );

    chain.add( dummy1 );
    chain.add( dummy2 );
    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }

  /**
   * This is a case when we have 2 transforms, but partitioned differently
   *
   * @throws HopPluginException
   */
  private void prepareTransformMetas_cl1_cl2() throws HopPluginException {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );

    PartitionSchema schema1 = new PartitionSchema( "p1", Arrays.asList( new String[] { PID1, PID2 } ) );
    PartitionSchema schema2 = new PartitionSchema( "p2", Arrays.asList( new String[] { PID1, PID2 } ) );

    TransformPartitioningMeta partMeta1 = new TransformPartitioningMeta( "Mirror to all partitions", schema1 );
    TransformPartitioningMeta partMeta2 = new TransformPartitioningMeta( "Mirror to all partitions", schema2 );
    partMeta1.setPartitionSchema( schema1 );
    partMeta2.setPartitionSchema( schema2 );

    dummy1.setTransformPartitioningMeta( partMeta1 );
    dummy2.setTransformPartitioningMeta( partMeta2 );

    chain.add( dummy1 );
    chain.add( dummy2 );
    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }

  /**
   * This is a case when first transform running 2 copies and next is partitioned one.
   *
   * @throws HopPluginException
   */
  private void prepareTransformMetas_x2_cl1() throws HopPluginException {
    TransformMeta dummy1 = new TransformMeta( ONE, null );
    TransformMeta dummy2 = new TransformMeta( TWO, null );

    PartitionSchema schema1 = new PartitionSchema( "p1", Arrays.asList( new String[] { PID1, PID2 } ) );
    TransformPartitioningMeta partMeta1 = new TransformPartitioningMeta( "Mirror to all partitions", schema1 );

    dummy2.setTransformPartitioningMeta( partMeta1 );
    dummy1.setCopies( 2 );

    chain.add( dummy1 );
    chain.add( dummy2 );
    for ( TransformMeta item : chain ) {
      item.setTransformMetaInterface( new DummyMeta() );
    }
  }
}
