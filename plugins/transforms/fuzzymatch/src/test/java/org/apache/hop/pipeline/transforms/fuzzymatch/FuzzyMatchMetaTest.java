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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;

/** Unit test for {@link FuzzyMatchMeta} */
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
    assertFalse(meta.isCaseSensitive());
    assertTrue(meta.isCloserValue());
    assertEquals("0", meta.getMinimalValue());
    assertEquals("1", meta.getMaximalValue());
    assertEquals(",", meta.getSeparator());
    assertEquals(FuzzyMatchMeta.Algorithm.SOUNDEX, meta.getAlgorithm());
    assertEquals(1, meta.getLookupValues().size());
    assertEquals("name", meta.getLookupValues().get(0).getName());
    assertEquals("lookupName", meta.getLookupValues().get(0).getRename());
  }

  @Test
  void testGetFieldsRename() throws Exception {
    FuzzyMatchMeta meta = new FuzzyMatchMeta();
    meta.setCloserValue(true);
    meta.setAlgorithm(FuzzyMatchMeta.Algorithm.JARO_WINKLER);
    meta.setOutputMatchField("match");
    meta.setOutputValueField("value");

    String oldName = "old_name";
    String newName = "new_name";
    String noChangeName = "noChangeName";

    // lookup name="oldName", rename="newName"
    FuzzyMatchMeta.FMLookupValue lookupValue = new FuzzyMatchMeta.FMLookupValue(oldName, newName);
    FuzzyMatchMeta.FMLookupValue noChange = new FuzzyMatchMeta.FMLookupValue(noChangeName, null);
    meta.setLookupValues(List.of(lookupValue, noChange));

    // input main row meta
    IRowMeta inputRowMeta = new RowMeta();

    // lookup info row meta
    IRowMeta lookupRowMeta = new RowMeta();
    lookupRowMeta.addValueMeta(new ValueMetaString(oldName));
    lookupRowMeta.addValueMeta(new ValueMetaString(noChangeName));

    IRowMeta[] info = new IRowMeta[] {lookupRowMeta};

    // execute getFields methods.
    meta.getFields(inputRowMeta, "FuzzyMatch", info, null, new Variables(), null);

    // valid rename
    IValueMeta result = inputRowMeta.searchValueMeta(newName);
    assertNotNull(result);
    assertEquals(newName, result.getName());
    assertEquals("FuzzyMatch", result.getOrigin());

    // not change.
    result = inputRowMeta.searchValueMeta(noChangeName);
    assertNotNull(result);
    assertEquals(noChangeName, result.getName());
  }
}
