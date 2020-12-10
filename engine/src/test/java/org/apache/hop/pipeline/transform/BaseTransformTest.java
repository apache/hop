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

package org.apache.hop.pipeline.transform;

import org.apache.commons.validator.Var;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.SingleRowRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.NonAccessibleFileObject;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.BasePartitioner;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BaseTransformTest {
  private TransformMockHelper<ITransformMeta, ITransformData> mockHelper;

  @Mock IRowHandler rowHandler;

  @Before
  public void setup() throws Exception {
    mockHelper =
        new TransformMockHelper<>("BASE TRANSFORM", ITransformMeta.class, ITransformData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
  }

  @After
  public void tearDown() {
    mockHelper.cleanUp();
  }

  /**
   * This test checks that data from one non-partitioned transform copies to 2 partitioned
   * transforms right.
   *
   * @throws HopException
   */
  @Test
  @Ignore // Impossible to figure out, move to integration tests
  public void testBaseTransformPutRowLocalSpecialPartitioning() throws HopException {
    List<TransformMeta> transformMetas = new ArrayList<>();
    transformMetas.add(mockHelper.transformMeta);
    transformMetas.add(mockHelper.transformMeta);
    TransformPartitioningMeta transformPartitioningMeta = spy(new TransformPartitioningMeta());
    BasePartitioner partitioner = mock(BasePartitioner.class);

    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenAnswer(
            (Answer<ILogChannel>)
                invocation -> {
                  ((BaseTransform) invocation.getArguments()[0]).getLogLevel();
                  return mockHelper.iLogChannel;
                });
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.pipelineMeta.findNextTransforms(any(TransformMeta.class)))
        .thenReturn(transformMetas);
    when(mockHelper.transformMeta.isPartitioned())
      .thenReturn( true );
    when(mockHelper.transformMeta.getTransformPartitioningMeta())
        .thenReturn(transformPartitioningMeta);

    when(transformPartitioningMeta.getPartitioner()).thenReturn(partitioner);
    when(partitioner.getNrPartitions()).thenReturn(2);

    Object object0 = "name0";
    IValueMeta meta0 = new ValueMetaString(object0.toString());

    Object object1 = "name1";
    IValueMeta meta2 = new ValueMetaString(object1.toString());

    IRowMeta rowMeta0 = new RowMeta();
    rowMeta0.addValueMeta(meta0);

    Object[] objects0 = {object0};

    IRowMeta rowMeta1 = new RowMeta();
    rowMeta1.addValueMeta(meta2);

    Object[] objects1 = {object1};

    // IVariables variables = mockHelper.pipeline;

    when(transformPartitioningMeta.getPartition(any(), eq(rowMeta0), eq(objects0))).thenReturn(0);
    when(transformPartitioningMeta.getPartition(any(), eq(rowMeta1), eq(objects1))).thenReturn(1);

    BlockingRowSet[] rowSet = {
      new BlockingRowSet(2), new BlockingRowSet(2), new BlockingRowSet(2), new BlockingRowSet(2)
    };
    List<IRowSet> outputRowSets = new ArrayList<>();
    outputRowSets.addAll(Arrays.asList(rowSet));

    LocalPipelineEngine pipeline = new LocalPipelineEngine();

    BaseTransform<ITransformMeta, ITransformData> baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            pipeline
        );
    baseTransform.setStopped(false);
    baseTransform.setRepartitioning(TransformPartitioningMeta.PARTITIONING_METHOD_SPECIAL);
    baseTransform.setOutputRowSets(outputRowSets);
    baseTransform.putRow(rowMeta0, objects0);
    baseTransform.putRow(rowMeta1, objects1);

    assertEquals(object0, baseTransform.getOutputRowSets().get(0).getRow()[0]);
    assertEquals(object1, baseTransform.getOutputRowSets().get(1).getRow()[0]);
    assertEquals(object0, baseTransform.getOutputRowSets().get(2).getRow()[0]);
    assertEquals(object1, baseTransform.getOutputRowSets().get(3).getRow()[0]);
  }

  @Test
  public void testBaseTransformGetLogLevelWontThrowNPEWithNullLog() {
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenAnswer(
            (Answer<ILogChannel>)
                invocation -> {
                  ((BaseTransform) invocation.getArguments()[0]).getLogLevel();
                  return mockHelper.iLogChannel;
                });
    new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline)
        .getLogLevel();
  }

  @Test
  public void testTransformListenersConcurrentModification() throws InterruptedException {
    // Create a base transform
    final BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);

    // Create thread to dynamically add listeners
    final AtomicBoolean done = new AtomicBoolean(false);
    Thread addListeners =
        new Thread() {
          @Override
          public void run() {
            while (!done.get()) {
              baseTransform.addTransformFinishedListener(mock(ITransformFinishedListener.class));
              synchronized (done) {
                done.notify();
              }
            }
          }
        };

    // Mark start and stop while listeners are being added
    try {
      addListeners.start();

      // Allow a few listeners to be added
      synchronized (done) {
        while (baseTransform.getTransformFinishedListeners().size() < 20) {
          done.wait();
        }
      }

      baseTransform.markStart();

      // Allow more listeners to be added
      synchronized (done) {
        while (baseTransform.getTransformFinishedListeners().size() < 100) {
          done.wait();
        }
      }

      baseTransform.markStop();

    } finally {
      // Close addListeners thread
      done.set(true);
      addListeners.join();
    }
  }

  @Test
  public void resultFilesMapIsSafeForConcurrentModification() throws Exception {
    final BaseTransform<ITransformMeta, ITransformData> transform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);

    final AtomicBoolean complete = new AtomicBoolean(false);

    final int FILES_AMOUNT = 10 * 1000;
    Thread filesProducer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < FILES_AMOUNT; i++) {
                  transform.addResultFile(
                      new ResultFile(
                          0, new NonAccessibleFileObject(Integer.toString(i)), null, null));
                  try {
                    Thread.sleep(1);
                  } catch (Exception e) {
                    fail(e.getMessage());
                  }
                }
              } finally {
                complete.set(true);
              }
            });

    filesProducer.start();
    try {
      while (!complete.get()) {
        for (Map.Entry<String, ResultFile> entry : transform.getResultFiles().entrySet()) {
          entry.getKey();
        }
      }
    } finally {
      filesProducer.join();
    }
  }

  @Test
  public void outputRowMetasAreNotSharedAmongSeveralStreams() throws Exception {
    IRowSet rs1 = new SingleRowRowSet();
    IRowSet rs2 = new SingleRowRowSet();

    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    BaseTransform<ITransformMeta, ITransformData> baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setStopped(false);
    baseTransform.setRepartitioning(TransformPartitioningMeta.PARTITIONING_METHOD_NONE);
    baseTransform.setOutputRowSets(Arrays.asList(rs1, rs2));

    for (IRowSet rowSet : baseTransform.getOutputRowSets()) {
      assertNull("RowMeta should be null, since no calls were done", rowSet.getRowMeta());
    }

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("string"));
    rowMeta.addValueMeta(new ValueMetaInteger("integer"));

    baseTransform.putRow(rowMeta, new Object[] {"a", 1});

    IRowMeta meta1 = rs1.getRowMeta();
    IRowMeta meta2 = rs2.getRowMeta();
    assertNotNull(meta1);
    assertNotNull(meta2);
    // content is same
    for (IValueMeta meta : meta1.getValueMetaList()) {
      assertTrue(meta.getName(), meta2.exists(meta));
    }
    // whereas instances differ
    assertFalse(meta1 == meta2);
  }

  @Test
  public void getRowWithRowHandler() throws HopException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);
    baseTransform.getRow();
    verify(rowHandler, times(1)).getRow();
  }

  @Test
  public void putRowWithRowHandler() throws HopException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);

    IRowMeta iRowMeta = mock(IRowMeta.class);
    Object[] objects = new Object[] {"foo", "bar"};
    baseTransform.putRow(iRowMeta, objects);
    verify(rowHandler, times(1)).putRow(iRowMeta, objects);
  }

  @Test
  public void putErrorWithRowHandler() throws HopException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);
    IRowMeta iRowMeta = mock(IRowMeta.class);
    Object[] objects = new Object[] {"foo", "bar"};
    baseTransform.putError(iRowMeta, objects, 3l, "desc", "field1,field2", "errorCode");
    verify(rowHandler, times(1))
        .putError(iRowMeta, objects, 3l, "desc", "field1,field2", "errorCode");
  }

  @Test
  public void putGetFromPutToDefaultRowHandlerMethods() throws HopException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);

    baseTransform.setRowHandler(rowHandlerWithDefaultMethods());

    IRowMeta iRowMeta = mock(IRowMeta.class);
    Object[] objects = new Object[] {"foo", "bar"};

    try {
      baseTransform.putRowTo(iRowMeta, objects, new QueueRowSet());
      fail("Expected default exception for putRowTo");
    } catch (UnsupportedOperationException uoe) {
      assertTrue(uoe.getMessage().contains(this.getClass().getName()));
    }
    try {
      baseTransform.getRowFrom(new QueueRowSet());
      fail("Expected default exception for getRowFrom");
    } catch (UnsupportedOperationException uoe) {
      assertTrue(uoe.getMessage().contains(this.getClass().getName()));
    }
  }

  private IRowHandler rowHandlerWithDefaultMethods() {
    return new IRowHandler() {
      @Override
      public Object[] getRow() throws HopException {
        return new Object[0];
      }

      @Override
      public void putRow(IRowMeta rowMeta, Object[] row) throws HopTransformException {}

      @Override
      public void putError(
          IRowMeta rowMeta,
          Object[] row,
          long nrErrors,
          String errorDescriptions,
          String fieldNames,
          String errorCodes)
          throws HopTransformException {}
    };
  }

  @Test
  public void notEmptyFieldName() throws HopTransformException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBase("name", IValueMeta.TYPE_INTEGER));

    baseTransform.putRow(rowMeta, new Object[] {0});
  }

  @Test(expected = HopTransformException.class)
  public void nullFieldName() throws HopTransformException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBase(null, IValueMeta.TYPE_INTEGER));

    baseTransform.putRow(rowMeta, new Object[] {0});
  }

  @Test(expected = HopTransformException.class)
  public void emptyFieldName() throws HopTransformException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBase("", IValueMeta.TYPE_INTEGER));

    baseTransform.putRow(rowMeta, new Object[] {0});
  }

  @Test(expected = HopTransformException.class)
  public void blankFieldName() throws HopTransformException {
    BaseTransform baseTransform =
        new BaseTransform(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    baseTransform.setRowHandler(rowHandler);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBase("  ", IValueMeta.TYPE_INTEGER));

    baseTransform.putRow(rowMeta, new Object[] {0});
  }

  @Test
  public void testGetRowSafeModeEnabled() throws HopException {
    Pipeline pipelineMock = spy(new LocalPipelineEngine());
    when(pipelineMock.isSafeModeEnabled()).thenReturn(true);
    BaseTransform baseTransformSpy =
        spy(
            new BaseTransform(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                mockHelper.iTransformData,
                0,
                mockHelper.pipelineMeta,
                pipelineMock));
    doNothing().when(baseTransformSpy).waitUntilPipelineIsStarted();

    BlockingRowSet rowSet = new BlockingRowSet(1);
    List<IValueMeta> valueMetaList =
        Arrays.asList(new ValueMetaInteger("x"), new ValueMetaString("a"));
    RowMeta rowMeta = new RowMeta();
    rowMeta.setValueMetaList(valueMetaList);
    final Object[] row = new Object[] {};
    rowSet.putRow(rowMeta, row);

    baseTransformSpy.setInputRowSets(Arrays.asList(rowSet));
    doReturn(rowSet).when(baseTransformSpy).currentInputStream();

    baseTransformSpy.getRow();

    verify(mockHelper.pipelineMeta, times(1))
        .checkRowMixingStatically(any(IVariables.class), any(TransformMeta.class), anyObject());
  }
}
