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

package org.apache.hop.pipeline.transforms.combinationlookup;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.util.HashMap;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CombinationLookupTest {

  private CombinationLookupMeta meta;
  private CombinationLookupData data;
  private PipelineMeta pipelineMeta;
  private CombinationLookup transform;
  private Database db;
  private RowMeta rowMeta;

  @BeforeAll
  static void setUpEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    meta = new CombinationLookupMeta();
    meta.setConnectionName("dwh");
    meta.getFields().getKeyFields().add(new KeyField("country", "country"));
    meta.getFields().getKeyFields().add(new KeyField("status", "status"));
    data = new CombinationLookupData();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    pipelineMeta = spy(new PipelineMeta());
    pipelineMeta.addTransform(transformMeta);
    transform = new CombinationLookup(transformMeta, meta, data, 0, pipelineMeta, null);

    rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("country"));
    rowMeta.addValueMeta(new ValueMetaString("status"));
    rowMeta.addValueMeta(new ValueMetaInteger("hash"));

    db = mock(Database.class);
    when(db.getDatabaseMeta()).thenReturn(mock(DatabaseMeta.class));
    data.db = db;
    data.prepStatementLookup = mock(PreparedStatement.class);
    data.keynrs = new int[] {0, 1};
    data.hashRowMeta = new RowMeta();
    data.hashRowMeta.addValueMeta(rowMeta.getValueMeta(0));
    data.hashRowMeta.addValueMeta(rowMeta.getValueMeta(1));
    data.outputRowMeta = rowMeta.clone();
    data.outputRowMeta.addValueMeta(new ValueMetaInteger("tk"));
    data.cache = new HashMap<>();
  }

  /** The connection was loaded from the metadata for every key of every row (B418, 200x). */
  @Test
  void cacheHitDoesNotLoadTheConnectionOrQueryTheDatabase() throws Exception {
    data.cache.put(new RowMetaAndData(data.hashRowMeta, "BE", "open"), 7L);

    for (int i = 0; i < 3; i++) {
      Object[] output = transform.lookupValues(rowMeta, new Object[] {"BE", "open", 0L});
      assertEquals(7L, output[3]);
    }

    verify(pipelineMeta, never()).findDatabase(anyString());
    verify(pipelineMeta, never()).findDatabase(anyString(), any());
    verify(pipelineMeta, never()).findDatabase(anyString(), any(), anyBoolean());
    verify(db, never()).setValues(any(IRowMeta.class), any(Object[].class), any());
    verify(db, never()).getLookup(any(PreparedStatement.class));
  }

  @Test
  void cacheMissBindsTheLookupRowAndCachesTheKey() throws Exception {
    data.lookupRowMeta = new RowMeta();
    for (int i = 0; i < 4; i++) {
      data.lookupRowMeta.addValueMeta(new ValueMetaString("lookup" + i));
    }
    data.nullCheckFlags = new boolean[] {true, false};
    RowMeta returnRowMeta = new RowMeta();
    returnRowMeta.addValueMeta(new ValueMetaInteger("tk"));
    when(db.getLookup(any(PreparedStatement.class))).thenReturn(new Object[] {5L});
    when(db.getReturnRowMeta()).thenReturn(returnRowMeta);

    Object[] row = new Object[] {"BE", "open", 0L};
    assertEquals(5L, transform.lookupValues(rowMeta, row)[3]);
    assertEquals(5L, transform.lookupValues(rowMeta, row)[3]);

    ArgumentCaptor<Object[]> lookupRow = ArgumentCaptor.forClass(Object[].class);
    verify(db, times(1)).setValues(any(IRowMeta.class), lookupRow.capture(), any());
    // KEYi = ? and the IS NULL check: a flag where the database casts it, else the value
    assertArrayEquals(new Object[] {"BE", "NotNull", "open", "open"}, lookupRow.getValue());
    verify(db, times(1)).getLookup(any(PreparedStatement.class));
  }

  @Test
  void calculatedHashIsAlsoTheCacheHash() throws Exception {
    meta.setUseHash(true);
    data.cache.put(new RowMetaAndData(data.hashRowMeta.clone(), "BE", "open"), 7L);
    data.hashRowMeta = spy(data.hashRowMeta);

    Object[] output = transform.lookupValues(rowMeta, new Object[] {"BE", "open", 0L});

    assertEquals(7L, output[3]);
    verify(data.hashRowMeta, times(1)).hashCode(any(Object[].class));
  }

  /** A hash read from the stream can be any number, the cache keeps the key fields' hash code. */
  @Test
  void streamHashIsNotTheCacheHash() throws Exception {
    meta.setUseHash(true);
    data.hashFieldNr = 2;
    data.cache.put(new RowMetaAndData(data.hashRowMeta, "BE", "open"), 7L);

    Object[] output = transform.lookupValues(rowMeta, new Object[] {"BE", "open", 42L});

    assertEquals(7L, output[3]);
    verify(db, never()).getLookup(any(PreparedStatement.class));
  }
}
