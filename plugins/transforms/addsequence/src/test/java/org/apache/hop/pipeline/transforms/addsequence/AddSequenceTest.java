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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.Counters;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AddSequenceTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<AddSequenceMeta, AddSequenceData> transformMockHelper;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    transformMockHelper =
        new TransformMockHelper<>("AddSequence", AddSequenceMeta.class, AddSequenceData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);
    when(transformMockHelper.transformMeta.getName()).thenReturn("AddSequence");
  }

  @AfterEach
  void tearDown() {
    // Clean up all counters to prevent test pollution
    try {
      Counters.getInstance().clear();
    } catch (Exception e) {
      // Ignore cleanup errors
    }
    transformMockHelper.cleanUp();
  }

  /** Test init() method with counter configuration */
  @Test
  void testInitWithCounter() {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("test_seq");
    when(transformMockHelper.iTransformMeta.getStartAt()).thenReturn("5");
    when(transformMockHelper.iTransformMeta.getIncrementBy()).thenReturn("2");
    when(transformMockHelper.iTransformMeta.getMaxValue()).thenReturn("1000");
    when(transformMockHelper.pipeline.getContainerId())
        .thenReturn("test-" + System.currentTimeMillis());

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    boolean result = addSequence.init();

    assertTrue(result);
    assertNotNull(transformMockHelper.iTransformData.counter);
    assertEquals(5L, transformMockHelper.iTransformData.start);
    assertEquals(2L, transformMockHelper.iTransformData.increment);
    assertEquals(1000L, transformMockHelper.iTransformData.maximum);

    addSequence.dispose();
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of("invalid", "2", "1000", "invalid startAt"),
        Arguments.of("1", "invalid", "1000", "invalid incrementBy"),
        Arguments.of("1", "1", "invalid", "invalid maxValue"));
  }

  /** Test init() method with invalid start value */
  @MethodSource("invalidConfigs")
  @ParameterizedTest(name = "should fail init() when {3}")
  void testInitWithInvalidStartValue(
      String startAt, String incrementBy, String maxValue, String desc) {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("test_seq");
    when(transformMockHelper.iTransformMeta.getStartAt()).thenReturn(startAt);
    when(transformMockHelper.iTransformMeta.getIncrementBy()).thenReturn(incrementBy);
    when(transformMockHelper.iTransformMeta.getMaxValue()).thenReturn(maxValue);

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    boolean result = addSequence.init();

    assertFalse(result, "init() should return false for " + desc);
  }

  /** Test dispose() method cleans up counter resources */
  @Test
  void testDisposeWithCounter() {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("test_seq");
    when(transformMockHelper.iTransformMeta.getStartAt()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.getIncrementBy()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.getMaxValue()).thenReturn("100");
    when(transformMockHelper.pipeline.getContainerId())
        .thenReturn("test-" + System.currentTimeMillis());

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(addSequence.init());
    assertNotNull(transformMockHelper.iTransformData.counter);

    addSequence.dispose();

    assertNull(transformMockHelper.iTransformData.counter);
  }

  /** Test init() method with database configuration fails when connection is missing */
  @Test
  void testInitWithDatabaseMissingConnection() {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.getConnection()).thenReturn("");

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    boolean result = addSequence.init();

    assertFalse(result);
  }

  /** Test init() method with neither counter nor database fails */
  @Test
  void testInitWithNoMethod() {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(false);

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    boolean result = addSequence.init();

    assertFalse(result);
  }

  /** Test addSequence() method with counter */
  @Test
  void testAddSequenceMethodWithCounter() throws HopException {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("id");
    when(transformMockHelper.iTransformMeta.getStartAt()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.getIncrementBy()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.getMaxValue()).thenReturn("100");
    when(transformMockHelper.pipeline.getContainerId())
        .thenReturn("test-" + System.currentTimeMillis());

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(addSequence.init());

    // Create input row
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    Object[] inputRow = new Object[] {"Alice"};

    // Test addSequence method
    Object[] result = addSequence.addSequence(inputRowMeta, inputRow);

    assertNotNull(result);
    assertEquals("Alice", result[0]);
    assertEquals(1L, result[1]);

    // Test second call
    result = addSequence.addSequence(inputRowMeta, new Object[] {"Bob"});
    assertNotNull(result);
    assertEquals("Bob", result[0]);
    assertEquals(2L, result[1]);

    addSequence.dispose();
  }

  /** Test addSequence() method with database */
  @Test
  void testAddSequenceMethodWithDatabase() throws HopException {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("id");

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    // Mock database - NOTE: This doesn't work as expected
    // The database needs to be set AFTER init() or through a different mechanism
    Database db = mock(Database.class);
    when(db.getNextSequenceValue("public", "test_seq", "id")).thenReturn(100L, 101L);
    when(addSequence.getData().getDb()).thenReturn(db);
    transformMockHelper.iTransformData.setDb(db);
    transformMockHelper.iTransformData.realSchemaName = "public";
    transformMockHelper.iTransformData.realSequenceName = "test_seq";

    // Create input row
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    Object[] inputRow = new Object[] {"Charlie"};

    // Test addSequence method
    Object[] result = addSequence.addSequence(inputRowMeta, inputRow);

    assertNotNull(result);
    assertEquals("Charlie", result[0]);
    assertEquals(100L, result[1]);

    // Test second call
    result = addSequence.addSequence(inputRowMeta, new Object[] {"Dave"});
    assertNotNull(result);
    assertEquals("Dave", result[0]);
    assertEquals(101L, result[1]);

    addSequence.dispose();
  }

  /** Test processRow() method with counter-based sequence */
  @Test
  void testProcessRowWithCounter() throws Exception {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("id");
    when(transformMockHelper.iTransformMeta.getStartAt()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.getIncrementBy()).thenReturn("1");
    when(transformMockHelper.iTransformMeta.getMaxValue()).thenReturn("100");
    when(transformMockHelper.pipeline.getContainerId())
        .thenReturn("test-processrow-" + System.currentTimeMillis());

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    assertTrue(addSequence.init());

    // Set up input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaInteger("age"));
    addSequence.setInputRowMeta(inputRowMeta);

    // Spy on the transform to mock getRow()
    addSequence = spy(addSequence);
    doReturn(new Object[] {"Alice", 30L})
        .doReturn(new Object[] {"Bob", 25L})
        .doReturn(new Object[] {"Charlie", 35L})
        .doReturn(null)
        .when(addSequence)
        .getRow();

    // Execute processRow multiple times
    List<Object[]> result = PipelineTestingUtil.execute(addSequence, 3, false);

    // Verify results
    assertEquals(3, result.size());

    // First row
    assertEquals(3, result.size());
    assertEquals("Alice", result.get(0)[0]);
    assertEquals(30L, result.get(0)[1]);
    assertEquals(1L, result.get(0)[2]);

    // Second row
    assertEquals("Bob", result.get(1)[0]);
    assertEquals(25L, result.get(1)[1]);
    assertEquals(2L, result.get(1)[2]);

    // Third row
    assertEquals("Charlie", result.get(2)[0]);
    assertEquals(35L, result.get(2)[1]);
    assertEquals(3L, result.get(2)[2]);

    addSequence.dispose();
  }

  /** Test processRow() method with database-based sequence */
  @Test
  void testProcessRowWithDatabase() throws Exception {
    when(transformMockHelper.iTransformMeta.isCounterUsed()).thenReturn(false);
    when(transformMockHelper.iTransformMeta.isDatabaseUsed()).thenReturn(true);
    when(transformMockHelper.iTransformMeta.getValueName()).thenReturn("id");

    AddSequence addSequence =
        new AddSequence(
            transformMockHelper.transformMeta,
            transformMockHelper.iTransformMeta,
            transformMockHelper.iTransformData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);

    // Mock database - NOTE: This approach doesn't work with spy()
    Database db = mock(Database.class);
    when(db.getNextSequenceValue("public", "my_seq", "id")).thenReturn(100L, 101L, 102L);
    when(addSequence.getData().getDb()).thenReturn(db);
    transformMockHelper.iTransformData.setDb(db);
    transformMockHelper.iTransformData.realSchemaName = "public";
    transformMockHelper.iTransformData.realSequenceName = "my_seq";

    // Set up input row meta
    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    addSequence.setInputRowMeta(inputRowMeta);

    // Spy on the transform to mock getRow()
    addSequence = spy(addSequence);
    doReturn(new Object[] {"User1"})
        .doReturn(new Object[] {"User2"})
        .doReturn(new Object[] {"User3"})
        .doReturn(null)
        .when(addSequence)
        .getRow();

    // Execute processRow multiple times
    List<Object[]> result = PipelineTestingUtil.execute(addSequence, 3, false);

    // Verify results
    assertEquals(3, result.size());

    // First row
    assertEquals("User1", result.get(0)[0]);
    assertEquals(100L, result.get(0)[1]);

    // Second row
    assertEquals("User2", result.get(1)[0]);
    assertEquals(101L, result.get(1)[1]);

    // Third row
    assertEquals("User3", result.get(2)[0]);
    assertEquals(102L, result.get(2)[1]);

    addSequence.dispose();
  }
}
