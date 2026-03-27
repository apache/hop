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
package org.apache.hop.pipeline.transforms.streamlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamLookupMetaTest {
  @BeforeEach
  void beforeEach() throws Exception {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @Test
  void testSerializationRoundTrip() throws Exception {
    StreamLookupMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/stream-lookup.xml", StreamLookupMeta.class);

    assertTrue(meta.isMemoryPreservationActive());
    assertTrue(meta.isUsingIntegerPair());
    assertTrue(meta.isUsingSortedList());

    assertEquals(2, meta.getLookup().getMatchKeys().size());
    StreamLookupMeta.MatchKey k = meta.getLookup().getMatchKeys().getFirst();
    assertEquals("keyStream1", k.getKeyStream());
    assertEquals("keyLookup1", k.getKeyLookup());

    k = meta.getLookup().getMatchKeys().getLast();
    assertEquals("keyStream2", k.getKeyStream());
    assertEquals("keyLookup2", k.getKeyLookup());

    assertEquals(3, meta.getLookup().getReturnValues().size());
    StreamLookupMeta.ReturnValue v = meta.getLookup().getReturnValues().getFirst();
    assertEquals("returnValue1", v.getValue());
    assertEquals("value1", v.getValueName());
    assertTrue(StringUtils.isEmpty(v.getValueDefault()));
    assertEquals(IValueMeta.TYPE_STRING, v.getValueDefaultType());

    v = meta.getLookup().getReturnValues().get(1);
    assertEquals("returnValue2", v.getValue());
    assertEquals("value2", v.getValueName());
    assertEquals("123", v.getValueDefault());
    assertEquals(IValueMeta.TYPE_INTEGER, v.getValueDefaultType());

    v = meta.getLookup().getReturnValues().getLast();
    assertEquals("returnValue3", v.getValue());
    assertEquals("value3", v.getValueName());
    assertEquals("true", v.getValueDefault());
    assertEquals(IValueMeta.TYPE_BOOLEAN, v.getValueDefaultType());
  }
}
