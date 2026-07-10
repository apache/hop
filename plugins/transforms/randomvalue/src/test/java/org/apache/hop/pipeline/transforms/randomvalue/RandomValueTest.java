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
package org.apache.hop.pipeline.transforms.randomvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Unit tests for {@link RandomValue}. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class RandomValueTest {

  private static final Pattern UUID_PATTERN =
      Pattern.compile(
          "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", Pattern.CASE_INSENSITIVE);

  private TransformMockHelper<RandomValueMeta, RandomValueData> mockHelper;

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("RANDOM_VALUE", RandomValueMeta.class, RandomValueData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
    when(mockHelper.pipeline.isStopped()).thenReturn(false);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void processRowSingleRowModeProducesOneOutputRowAndStops() throws HopException {
    RandomValueMeta meta = metaWithFields(field("num", RandomValueMeta.RandomType.NUMBER));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    assertTrue(transform.init());
    assertFalse(transform.processRow());

    Object[] output = outputRowSet.getRow();
    assertNotNull(output);
    assertEquals(1, data.outputRowMeta.size());
    assertNotNull(output[0]);
    assertTrue(data.outputRowMeta.indexOfValue("num") >= 0);
  }

  @Test
  void processRowSingleRowModeSeedProducesDeterministicValues() throws HopException {
    RandomValueMeta meta =
        metaWithSeed(
            "12345",
            field("num", RandomValueMeta.RandomType.NUMBER),
            field("int", RandomValueMeta.RandomType.INTEGER),
            field("str", RandomValueMeta.RandomType.STRING));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    Random r = new Random(Const.toLong("12345", 0));
    double expectedNumber = r.nextDouble();
    long expectedInteger = (long) r.nextInt();
    String expectedString = Long.toString(Math.abs(r.nextLong()), 32);

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    assertTrue(transform.init());
    transform.processRow();

    Object[] output = outputRowSet.getRow();
    assertEquals(expectedNumber, output[0]);
    assertEquals(expectedInteger, output[1]);
    assertEquals(expectedString, output[2]);
  }

  @Test
  void processRowReadsInputAndAppendsRandomValues() throws HopException {
    RandomValueMeta meta =
        metaWithSeed(
            "99",
            field("num", RandomValueMeta.RandomType.NUMBER),
            field("str", RandomValueMeta.RandomType.STRING));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any()))
        .thenReturn(List.of(mock(TransformMeta.class)));

    RowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("input"));

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);
    transform.setInputRowMeta(inputRowMeta);
    transform.addRowSetToInputRowSets(
        mockHelper.getMockInputRowSet(new Object[][] {{"row-1"}, {"row-2"}}));
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    assertTrue(transform.init());
    assertTrue(transform.processRow());
    Object[] firstOutput = outputRowSet.getRow();
    assertEquals("row-1", firstOutput[0]);
    assertNotNull(firstOutput[1]);
    assertNotNull(firstOutput[2]);

    assertTrue(transform.processRow());
    Object[] secondOutput = outputRowSet.getRow();
    assertEquals("row-2", secondOutput[0]);
    assertNotNull(secondOutput[1]);
    assertNotNull(secondOutput[2]);

    assertFalse(transform.processRow());
    assertNull(outputRowSet.getRow());
  }

  @ParameterizedTest
  @EnumSource(
      value = RandomValueMeta.RandomType.class,
      names = {"NONE"},
      mode = EnumSource.Mode.EXCLUDE)
  void processRowSingleRowModeSupportsAllConfiguredTypes(RandomValueMeta.RandomType type)
      throws HopException {
    RandomValueMeta meta = metaWithFields(field("value", type));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    assertTrue(transform.init());
    transform.processRow();

    Object[] output = outputRowSet.getRow();
    assertNotNull(output);
    assertEquals(1, data.outputRowMeta.size());
    assertNotNull(output[0], "Expected value for type " + type);

    switch (type) {
      case NUMBER -> assertInstanceOf(Double.class, output[0]);
      case INTEGER -> assertInstanceOf(Long.class, output[0]);
      case STRING, UUID, UUID4, HMAC_MD5, HMAC_SHA1, HMAC_SHA256, HMAC_SHA512, HMAC_SHA384 ->
          assertInstanceOf(String.class, output[0]);
      default -> throw new AssertionError("Unhandled type: " + type);
    }

    if (type == RandomValueMeta.RandomType.UUID || type == RandomValueMeta.RandomType.UUID4) {
      assertTrue(UUID_PATTERN.matcher((String) output[0]).matches());
    }
    if (type.name().startsWith("HMAC_")) {
      assertFalse(((String) output[0]).isEmpty());
    }
  }

  @Test
  void initInitializesRandomGeneratorOnlyWhenNeeded() {
    RandomValueMeta meta = metaWithFields(field("uuid", RandomValueMeta.RandomType.UUID));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);

    assertTrue(transform.init());
    assertNull(data.randomGenerator);
    assertNull(data.u4);
  }

  @Test
  void initInitializesUuid4UtilWhenNeeded() {
    RandomValueMeta meta = metaWithFields(field("uuid4", RandomValueMeta.RandomType.UUID4));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);

    assertTrue(transform.init());
    assertNotNull(data.u4);
  }

  @Test
  void initInitializesHmacGeneratorsWhenNeeded() {
    RandomValueMeta meta =
        metaWithFields(
            field("md5", RandomValueMeta.RandomType.HMAC_MD5),
            field("sha1", RandomValueMeta.RandomType.HMAC_SHA1),
            field("sha256", RandomValueMeta.RandomType.HMAC_SHA256),
            field("sha384", RandomValueMeta.RandomType.HMAC_SHA384),
            field("sha512", RandomValueMeta.RandomType.HMAC_SHA512));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);

    assertTrue(transform.init());
    assertNotNull(data.keyGenHmacMD5);
    assertNotNull(data.keyGenHmacSHA1);
    assertNotNull(data.keyGenHmacSHA256);
    assertNotNull(data.keyGenHmacSHA384);
    assertNotNull(data.keyGenHmacSHA512);
  }

  @Test
  void initSetsReadsRowsWhenPreviousTransformsExist() {
    RandomValueMeta meta = metaWithFields(field("num", RandomValueMeta.RandomType.NUMBER));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any()))
        .thenReturn(List.of(mock(TransformMeta.class)));

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);

    assertTrue(transform.init());
    assertTrue(data.readsRows);
    assertNotNull(data.randomGenerator);
  }

  @Test
  void processRowUuidValueHasStandardFormat() throws HopException {
    RandomValueMeta meta = metaWithFields(field("uuid", RandomValueMeta.RandomType.UUID));
    when(mockHelper.pipelineMeta.findPreviousTransforms(any())).thenReturn(new ArrayList<>());

    RandomValueData data = new RandomValueData();
    RandomValue transform = newTransform(meta, data);
    QueueRowSet outputRowSet = new QueueRowSet();
    transform.addRowSetToOutputRowSets(outputRowSet);

    assertTrue(transform.init());
    transform.processRow();

    String uuid = (String) outputRowSet.getRow()[0];
    assertNotNull(UUID.fromString(uuid));
  }

  private RandomValue newTransform(RandomValueMeta meta, RandomValueData data) {
    return new RandomValue(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private static RandomValueMeta metaWithFields(RandomValueMeta.RVField... fields) {
    return metaWithSeed(null, fields);
  }

  private static RandomValueMeta metaWithSeed(String seed, RandomValueMeta.RVField... fields) {
    RandomValueMeta meta = new RandomValueMeta();
    meta.setSeed(seed);
    for (RandomValueMeta.RVField field : fields) {
      meta.getFields().add(field);
    }
    return meta;
  }

  private static RandomValueMeta.RVField field(String name, RandomValueMeta.RandomType type) {
    RandomValueMeta.RVField field = new RandomValueMeta.RVField();
    field.setName(name);
    field.setType(type);
    return field;
  }
}
