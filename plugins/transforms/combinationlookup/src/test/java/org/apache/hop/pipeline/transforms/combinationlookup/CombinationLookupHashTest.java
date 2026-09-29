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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CombinationLookupHashTest {

  @BeforeAll
  static void setUp() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void calculatedHashMatchesTheRowHash() throws Exception {
    CombinationLookup transform = transform();
    RowMeta hashRowMeta = new RowMeta();
    hashRowMeta.addValueMeta(new ValueMetaString("key"));
    Object[] hashRow = new Object[] {"abc"};
    transform.getData().hashRowMeta = hashRowMeta;
    transform.getData().hashFieldNr = -1;

    RowMeta rowMeta = rowMeta();
    long expected = hashRowMeta.hashCode(hashRow);

    assertEquals(expected, transform.hashValue(rowMeta, new Object[] {"abc", 42L}, hashRow));
    assertNotEquals(42L, expected);
  }

  @Test
  void streamFieldReplacesTheCalculatedHash() throws Exception {
    CombinationLookup transform = transform();
    transform.getMeta().setUseHash(true);
    transform.getMeta().setHashFieldInStream("${HASH_FIELD}");
    transform.setVariable("HASH_FIELD", "hash");

    RowMeta rowMeta = rowMeta();
    transform.resolveHashField(rowMeta);
    assertEquals(1, transform.getData().hashFieldNr);

    RowMeta hashRowMeta = new RowMeta();
    hashRowMeta.addValueMeta(new ValueMetaString("key"));
    Object[] hashRow = new Object[] {"abc"};
    transform.getData().hashRowMeta = hashRowMeta;

    assertEquals(42L, transform.hashValue(rowMeta, new Object[] {"abc", 42L}, hashRow));
    assertEquals(-15L, transform.hashValue(rowMeta, new Object[] {"abc", -15L}, hashRow));
    assertNull(transform.hashValue(rowMeta, new Object[] {"abc", null}, hashRow));
  }

  @Test
  void emptyStreamFieldKeepsTheCalculatedHash() throws Exception {
    CombinationLookup transform = transform();
    transform.getMeta().setUseHash(true);
    transform.getMeta().setHashFieldInStream("");
    transform.resolveHashField(rowMeta());
    assertEquals(-1, transform.getData().hashFieldNr);

    transform.getMeta().setUseHash(false);
    transform.getMeta().setHashFieldInStream("hash");
    transform.resolveHashField(rowMeta());
    assertEquals(-1, transform.getData().hashFieldNr);
  }

  @Test
  void missingStreamFieldFails() {
    CombinationLookup transform = transform();
    transform.getMeta().setUseHash(true);
    transform.getMeta().setHashFieldInStream("missing");

    HopTransformException exception =
        assertThrows(HopTransformException.class, () -> transform.resolveHashField(rowMeta()));
    assertTrue(exception.getMessage().contains("missing"));
  }

  @Test
  void checkAcceptsAnIntegerStreamField() {
    CombinationLookupMeta meta = new CombinationLookupMeta();
    meta.setUseHash(true);
    meta.setHashFieldInStream("${HASH_FIELD}");
    Variables variables = new Variables();
    variables.setVariable("HASH_FIELD", "row_hash");
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaInteger("row_hash"));
    List<ICheckResult> remarks = new ArrayList<>();

    meta.checkHashField(remarks, variables, new TransformMeta("lookup", meta), prev);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
  }

  @Test
  void checkRejectsAMissingOrNonIntegerStreamField() {
    CombinationLookupMeta meta = new CombinationLookupMeta();
    meta.setUseHash(true);
    meta.setHashFieldInStream("row_hash");
    RowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("row_hash"));
    List<ICheckResult> remarks = new ArrayList<>();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);

    meta.checkHashField(remarks, new Variables(), transformMeta, prev);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());

    remarks.clear();
    prev.clear();
    meta.checkHashField(remarks, new Variables(), transformMeta, prev);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());

    remarks.clear();
    meta.setUseHash(false);
    meta.checkHashField(remarks, new Variables(), transformMeta, prev);
    assertTrue(remarks.isEmpty());
  }

  private static CombinationLookup transform() {
    CombinationLookupMeta meta = new CombinationLookupMeta();
    CombinationLookupData data = new CombinationLookupData();
    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);
    return new CombinationLookup(transformMeta, meta, data, 0, pipelineMeta, null);
  }

  private static RowMeta rowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("key"));
    rowMeta.addValueMeta(new ValueMetaInteger("hash"));
    return rowMeta;
  }
}
