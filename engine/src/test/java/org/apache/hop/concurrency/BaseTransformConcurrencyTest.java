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

package org.apache.hop.concurrency;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class BaseTransformConcurrencyTest {
  private static final String TRANSFORM_META = "TransformMeta";

  private BaseTransform<ITransformMeta, ITransformData> baseTransform;

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

    baseTransform = new BaseTransform( transformMeta, null, null, 0, pipelineMeta, spy( new LocalPipelineEngine() ) );

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

    baseTransform = new BaseTransform( transformMeta, null, null, 0, pipelineMeta, spy( new LocalPipelineEngine() ) );

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
      baseTransform.addRowSetToInputRowSets( mock( IRowSet.class ) );
      baseTransform.addRowSetToOutputRowSets( mock( IRowSet.class ) );
      return null;
    }
  }

  private class RowSetsTraverser extends StopOnErrorCallable<BaseTransform> {
    RowSetsTraverser( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    BaseTransform doCall() {
      for ( IRowSet rowSet : baseTransform.getInputRowSets() ) {
        rowSet.setRowMeta( mock( IRowMeta.class ) );
      }
      for ( IRowSet rowSet : baseTransform.getOutputRowSets() ) {
        rowSet.setRowMeta( mock( IRowMeta.class ) );
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
      baseTransform.addRowListener( mock( IRowListener.class ) );
      return null;
    }
  }

  private class RowListenersTraverser extends StopOnErrorCallable<BaseTransform> {
    RowListenersTraverser( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    BaseTransform doCall() throws Exception {
      for ( IRowListener rowListener : baseTransform.getRowListeners() ) {
        rowListener.rowWrittenEvent( mock( IRowMeta.class ), new Object[] {} );
      }
      return null;
    }
  }
}
