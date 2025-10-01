/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.addsequence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import org.apache.hop.core.Counter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.junit.jupiter.api.Test;

class AddSequenceDataTest {

  @Test
  void testDefaultConstructor() {
    AddSequenceData data = new AddSequenceData();

    assertNull(data.getDb());
    assertNull(data.getLookup());
    assertNull(data.outputRowMeta);
    assertNull(data.counter);
    assertEquals(0L, data.start);
    assertEquals(0L, data.increment);
    assertEquals(0L, data.maximum);
    assertNull(data.realSchemaName);
    assertNull(data.realSequenceName);
  }

  @Test
  void testSettersAndGetters() {
    AddSequenceData data = new AddSequenceData();

    Database db = mock(Database.class);
    data.setDb(db);
    assertEquals(db, data.getDb());

    data.setLookup("@@sequence:test");
    assertEquals("@@sequence:test", data.getLookup());

    Counter counter = mock(Counter.class);
    data.counter = counter;
    assertEquals(counter, data.counter);

    IRowMeta rowMeta = mock(IRowMeta.class);
    data.outputRowMeta = rowMeta;
    assertEquals(rowMeta, data.outputRowMeta);

    data.start = 10L;
    assertEquals(10L, data.start);

    data.increment = 2L;
    assertEquals(2L, data.increment);

    data.maximum = 100L;
    assertEquals(100L, data.maximum);

    data.realSchemaName = "public";
    assertEquals("public", data.realSchemaName);

    data.realSequenceName = "my_seq";
    assertEquals("my_seq", data.realSequenceName);
  }
}
