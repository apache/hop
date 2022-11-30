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

import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class FuzzyMatchMetaTest {

  @Test
  public void testSerialization() throws Exception {
    FuzzyMatchMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/fuzzy-match-transform.xml", FuzzyMatchMeta.class);

    Assert.assertEquals("Data grid", meta.getLookupTransformName());
    Assert.assertEquals("name", meta.getLookupField());
    Assert.assertEquals("name", meta.getMainStreamField());
    Assert.assertEquals("match", meta.getOutputMatchField());
    Assert.assertEquals("measure value", meta.getOutputValueField());
    Assert.assertEquals(false, meta.isCaseSensitive());
    Assert.assertEquals(true, meta.isCloserValue());
    Assert.assertEquals("0", meta.getMinimalValue());
    Assert.assertEquals("1", meta.getMaximalValue());
    Assert.assertEquals(",", meta.getSeparator());
    Assert.assertEquals(FuzzyMatchMeta.Algorithm.SOUNDEX, meta.getAlgorithm());
    Assert.assertEquals(1, meta.getLookupValues().size());
    Assert.assertEquals("name", meta.getLookupValues().get(0).getName());
    Assert.assertEquals("lookupName", meta.getLookupValues().get(0).getRename());
  }
}
