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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AddSequenceMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void testSerialization() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "valueName",
            "databaseUsed",
            "connection",
            "schemaName",
            "sequenceName",
            "counterUsed",
            "counterName",
            "startAt",
            "incrementBy",
            "maxValue");

    LoadSaveTester<AddSequenceMeta> loadSaveTester =
        new LoadSaveTester<>(
            AddSequenceMeta.class,
            attributes,
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<String, IFieldLoadSaveValidator<?>>(),
            new HashMap<String, IFieldLoadSaveValidator<?>>());
    loadSaveTester.testSerialization();
  }

  @Test
  void testSetDefault() {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setDefault();

    assertEquals("valuename", meta.getValueName());
    assertFalse(meta.isDatabaseUsed());
    assertTrue(meta.isCounterUsed());
    assertEquals("", meta.getSchemaName());
    assertEquals("SEQ_", meta.getSequenceName());
    assertNull(meta.getCounterName());
    assertEquals("1", meta.getStartAt());
    assertEquals("1", meta.getIncrementBy());
    assertEquals("999999999", meta.getMaxValue());
  }

  @Test
  void testGettersAndSetters() {
    AddSequenceMeta meta = new AddSequenceMeta();

    meta.setValueName("test_seq");
    assertEquals("test_seq", meta.getValueName());

    meta.setDatabaseUsed(true);
    assertTrue(meta.isDatabaseUsed());

    meta.setCounterUsed(false);
    assertFalse(meta.isCounterUsed());

    meta.setConnection("db_conn");
    assertEquals("db_conn", meta.getConnection());

    meta.setSchemaName("schema1");
    assertEquals("schema1", meta.getSchemaName());

    meta.setSequenceName("seq1");
    assertEquals("seq1", meta.getSequenceName());

    meta.setCounterName("counter1");
    assertEquals("counter1", meta.getCounterName());

    // Test string setters
    meta.setStartAt("10");
    assertEquals("10", meta.getStartAt());

    meta.setIncrementBy("5");
    assertEquals("5", meta.getIncrementBy());

    meta.setMaxValue("1000");
    assertEquals("1000", meta.getMaxValue());

    // Test long setters
    meta.setStartAt(100L);
    assertEquals("100", meta.getStartAt());

    meta.setIncrementBy(10L);
    assertEquals("10", meta.getIncrementBy());

    meta.setMaxValue(10000L);
    assertEquals("10000", meta.getMaxValue());
  }

  @Test
  void testClone() {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setValueName("test_value");
    meta.setDatabaseUsed(true);
    meta.setConnection("test_connection");
    meta.setStartAt("100");

    AddSequenceMeta cloned = (AddSequenceMeta) meta.clone();

    assertNotNull(cloned);
    assertEquals(meta.getValueName(), cloned.getValueName());
    assertEquals(meta.isDatabaseUsed(), cloned.isDatabaseUsed());
    assertEquals(meta.getConnection(), cloned.getConnection());
    assertEquals(meta.getStartAt(), cloned.getStartAt());
  }
}
