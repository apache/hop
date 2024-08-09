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
package org.apache.hop.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.junit.Before;
import org.junit.Test;

public class RowProducerTest {

  RowProducer rowProducer;
  ITransform iTransform;
  IRowSet rowSet;
  IRowMeta rowMeta;
  Object[] rowData;

  @Before
  public void setUp() {
    iTransform = mock(ITransform.class);
    rowSet = mock(IRowSet.class);
    rowProducer = new RowProducer(iTransform, rowSet);
    rowMeta = mock(IRowMeta.class);
    rowData = new Object[] {};
  }

  @Test
  public void testPutRow2Arg() {
    when(rowSet.putRowWait(
            any(IRowMeta.class), any(Object[].class), anyLong(), any(TimeUnit.class)))
        .thenReturn(true);
    rowProducer.putRow(rowMeta, rowData);
    verify(rowSet, times(1)).putRowWait(rowMeta, rowData, Long.MAX_VALUE, TimeUnit.DAYS);
    assertTrue(rowProducer.putRow(rowMeta, rowData, true));
  }

  @Test
  public void testPutRow3Arg() {
    when(rowSet.putRowWait(
            any(IRowMeta.class), any(Object[].class), anyLong(), any(TimeUnit.class)))
        .thenReturn(true);

    rowProducer.putRow(rowMeta, rowData, false);
    verify(rowSet, times(1)).putRow(rowMeta, rowData);
  }

  @Test
  public void testPutRowWait() {
    rowProducer.putRowWait(rowMeta, rowData, 1, TimeUnit.MILLISECONDS);
    verify(rowSet, times(1)).putRowWait(rowMeta, rowData, 1, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testFinished() {
    rowProducer.finished();
    verify(rowSet, times(1)).setDone();
  }

  @Test
  public void testGetSetRowSet() {
    assertEquals(rowSet, rowProducer.getRowSet());
    rowProducer.setRowSet(null);
    assertNull(rowProducer.getRowSet());
    IRowSet newRowSet = mock(IRowSet.class);
    rowProducer.setRowSet(newRowSet);
    assertEquals(newRowSet, rowProducer.getRowSet());
  }

  @Test
  public void testGetSetTransform() {
    assertEquals(iTransform, rowProducer.getTransform());
    rowProducer.setTransform(null);
    assertNull(rowProducer.getTransform());
    ITransform newTransform = mock(ITransform.class);
    rowProducer.setTransform(newTransform);
    assertEquals(newTransform, rowProducer.getTransform());
  }
}
