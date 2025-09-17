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

package org.apache.hop.pipeline.transforms.memgroupby;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MemoryGroupByDataTest {

  private MemoryGroupByData data = new MemoryGroupByData();

  @Mock private IRowMeta groupMeta;
  @Mock private IValueMeta valueMeta;

  @BeforeEach
  void setUp() throws Exception {
    data.groupMeta = groupMeta;
    when(groupMeta.size()).thenReturn(1);
    when(groupMeta.getValueMeta(anyInt())).thenReturn(valueMeta);
    when(valueMeta.convertToNormalStorageType(any()))
        .then(
            invocation -> {
              Object argument = invocation.getArguments()[0];
              return new String((byte[]) argument);
            });
  }

  @Test
  void hashEntryTest() {
    HashMap<MemoryGroupByData.HashEntry, String> map = new HashMap<>();

    byte[] byteValue1 = "key".getBytes();
    Object[] groupData1 = new Object[1];
    groupData1[0] = byteValue1;

    MemoryGroupByData.HashEntry hashEntry1 = data.getHashEntry(groupData1);
    map.put(hashEntry1, "value");

    byte[] byteValue2 = "key".getBytes();
    Object[] groupData2 = new Object[1];
    groupData2[0] = byteValue2;

    MemoryGroupByData.HashEntry hashEntry2 = data.getHashEntry(groupData2);

    String value = map.get(hashEntry2);

    assertEquals("value", value);
  }
}
