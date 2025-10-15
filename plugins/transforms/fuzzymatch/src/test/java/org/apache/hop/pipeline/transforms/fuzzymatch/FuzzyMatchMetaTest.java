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
package org.apache.hop.pipeline.transforms.fuzzymatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

class FuzzyMatchMetaTest {

  @Test
  void testSerialization() throws Exception {
    FuzzyMatchMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/fuzzy-match-transform.xml", FuzzyMatchMeta.class);

    assertEquals("Data grid", meta.getLookupTransformName());
    assertEquals("name", meta.getLookupField());
    assertEquals("name", meta.getMainStreamField());
    assertEquals("match", meta.getOutputMatchField());
    assertEquals("measure value", meta.getOutputValueField());
    assertEquals(false, meta.isCaseSensitive());
    assertEquals(true, meta.isCloserValue());
    assertEquals("0", meta.getMinimalValue());
    assertEquals("1", meta.getMaximalValue());
    assertEquals(",", meta.getSeparator());
    assertEquals(FuzzyMatchMeta.Algorithm.SOUNDEX, meta.getAlgorithm());
    assertEquals(1, meta.getLookupValues().size());
    assertEquals("name", meta.getLookupValues().get(0).getName());
    assertEquals("lookupName", meta.getLookupValues().get(0).getRename());
  }
}
