/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.concurrency;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.RowListener;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseTransformConcurrencyTest {
  private static final String TRANSFORM_META = "TransformMeta";

  private BaseTransform<TransformMetaInterface, TransformDataInterface> baseTransform;

  /**
   * Row listeners collection modifiers are exposed out of BaseTransform class,
   * whereas the collection traversal is happening on every row being processed.
   * <p>
   * We should be sure that modification of the collection will not throw a concurrent modification exception.
   */
  @Test
  public void testRowListeners() throws Exception {
    int modifiersAmount = 100;
    int traversersAmount = 100;

    TransformMeta transformMeta = mock( TransformMeta.class );
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    when( transformMeta.getName() ).thenReturn( TRANSFORM_META );
    when( pipelineMeta.findTransform( TRANSFORM_META ) ).thenReturn( transformMeta );
    when( transformMeta.getTargetTransformPartitioningMeta() ).thenReturn( mock( TransformPartitioningMeta.class ) );

    baseTransform = new BaseTransform( transformMeta, null, 0, pipelineMeta, mock( Pipeline.class ) );

    AtomicBoolean condition = new AtomicBoolean( true );

    List<RowListenersModifier> rowListenersModifiers = new ArrayList<>();
    for ( int i = 0; i < modifiersAmount; i++ ) {
      rowListenersModifiers.add( new RowListenersModifier( condition ) );
    }
    List<RowListenersTraverser> rowListenersTraversers = new ArrayList<>();
    for ( int i = 0; i < traversersAmount; i++ ) {
      rowListenersTraversers.add( new RowListenersTraverser( condition ) );
    }

    ConcurrencyTestRunner<?, ?> runner =
      new ConcurrencyTestRunner<Object, Object>( rowListenersModifiers, rowListenersTraversers, condition );
    runner.runConcurrentTest();

    runner.checkNoExceptionRaised();
  }

  /**
   * Row sets collection modifiers are exposed out of BaseTransform class,
   * whereas the collection traversal is happening on every row being processed.
   * <p>
   * We should be sure that modification of the collection will not throw a concurrent modification exception.
   */
  @Test
  public void testInputOutputRowSets() throws Exception {
    int modifiersAmount = 100;
    int traversersAmount = 100;

    TransformMeta transformMeta = mock( TransformMeta.class );
    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    when( transformMeta.getName() ).thenReturn( TRANSFORM_META );
    when( pipelineMeta.findTransform( TRANSFORM_META ) ).thenReturn( transformMeta );
    when( transformMeta.getTargetTransformPartitioningMeta() ).thenReturn( mock( TransformPartitioningMeta.class ) );

    baseTransform = new BaseTransform( transformMeta, null, 0, pipelineMeta, mock( Pipeline.class ) );

    AtomicBoolean condition = new AtomicBoolean( true );

    List<RowSetsModifier> rowSetsModifiers = new ArrayList<>();
    for ( int i = 0; i < modifiersAmount; i++ ) {
      rowSetsModifiers.add( new RowSetsModifier( condition ) );
    }
    List<RowSetsTraverser> rowSetsTraversers = new ArrayList<>();
    for ( int i = 0; i < traversersAmount; i++ ) {
      rowSetsTraversers.add( new RowSetsTraverser( condition ) );
    }

    ConcurrencyTestRunner<?, ?> runner =
      new ConcurrencyTestRunner<Object, Object>( rowSetsModifiers, rowSetsTraversers, condition );
    runner.runConcurrentTest();

    runner.checkNoExceptionRaised();
  }

  private class RowSetsModifier extends StopOnErrorCallable<BaseTransform> {
    RowSetsModifier( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    BaseTransform doCall() {
      baseTransform.addRowSetToInputRowSets( mock( RowSet.class ) );
      baseTransform.addRowSetToOutputRowSets( mock( RowSet.class ) );
      return null;
    }
  }

  private class RowSetsTraverser extends StopOnErrorCallable<BaseTransform> {
    RowSetsTraverser( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    BaseTransform doCall() {
      for ( RowSet rowSet : baseTransform.getInputRowSets() ) {
        rowSet.setRowMeta( mock( RowMetaInterface.class ) );
      }
      for ( RowSet rowSet : baseTransform.getOutputRowSets() ) {
        rowSet.setRowMeta( mock( RowMetaInterface.class ) );
      }
      return null;
    }
  }

  private class RowListenersModifier extends StopOnErrorCallable<BaseTransform> {
    RowListenersModifier( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    BaseTransform doCall() {
      baseTransform.addRowListener( mock( RowListener.class ) );
      return null;
    }
  }

  private class RowListenersTraverser extends StopOnErrorCallable<BaseTransform> {
    RowListenersTraverser( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    BaseTransform doCall() throws Exception {
      for ( RowListener rowListener : baseTransform.getRowListeners() ) {
        rowListener.rowWrittenEvent( mock( RowMetaInterface.class ), new Object[] {} );
      }
      return null;
    }
  }
}
