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

package org.apache.hop.redis.transforms.redisinput;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.redis.client.IRedisCommands;
import org.apache.hop.redis.client.RedisClientSession;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.codec.RedisCodecs;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.redisinput.RedisInputData.Mapping;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RedisInputTest {

  private TransformMockHelper<RedisInputMeta, RedisInputData> mockHelper;
  private IRedisCommands commands;
  private RedisClientSession session;

  @BeforeAll
  static void initEnv() throws Exception {
    HopEnvironment.init();
  }

  @AfterAll
  static void resetEnv() {
    HopEnvironment.reset();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>("RedisInput", RedisInputMeta.class, RedisInputData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    commands = mock(IRedisCommands.class);
    session = mock(RedisClientSession.class);
    when(session.getCommands()).thenReturn(commands);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void toJsonArrayElementKeepsTextAndBase64EncodesBinary() {
    assertEquals("hello", RedisInput.toJsonArrayElement("hello"));
    assertEquals("12", RedisInput.toJsonArrayElement("12".getBytes(StandardCharsets.UTF_8)));
    byte[] binary = new byte[] {(byte) 0xFF, 0x00};
    assertEquals(Base64.getEncoder().encodeToString(binary), RedisInput.toJsonArrayElement(binary));
  }

  @Test
  void processRowReadsStringFromFieldKey() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    RedisInputData data = new RedisInputData();
    RedisInput transform = spy(createTransform(meta, data));

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("redisKey"));
    IRowMeta outputMeta = inputMeta.clone();
    outputMeta.addValueMeta(new ValueMetaString("outValue"));

    Mapping mapping = stringMapping(0, 1);
    data.setSession(session);
    data.setMappings(new Mapping[] {mapping});
    data.setOutputRowMeta(outputMeta);
    transform.first = false;

    byte[] keyBytes = "user:1".getBytes(StandardCharsets.UTF_8);
    when(commands.getValue(any(byte[].class))).thenReturn("alice".getBytes(StandardCharsets.UTF_8));

    doReturn(inputMeta).when(transform).getInputRowMeta();
    doReturn(new Object[] {"user:1"}).doReturn(null).when(transform).getRow();
    ArgumentCaptor<Object[]> outCaptor = ArgumentCaptor.forClass(Object[].class);
    doAnswer(inv -> null).when(transform).putRow(any(), outCaptor.capture());

    assertTrue(transform.processRow());
    assertArrayEquals(keyBytes, captureKey(commands));
    assertEquals("alice", outCaptor.getValue()[1]);
    assertFalse(transform.processRow());
  }

  @Test
  void processRowReadsHashWithLiteralKey() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    RedisInputData data = new RedisInputData();
    RedisInput transform = spy(createTransform(meta, data));

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("unused"));
    IRowMeta outputMeta = inputMeta.clone();
    outputMeta.addValueMeta(new ValueMetaString("email"));

    Mapping mapping = new Mapping();
    mapping.setStructure(RedisDataStructure.HASH);
    mapping.setKeyLiteral("profile");
    mapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setHashFieldLiteral("email");
    mapping.setHashFieldCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueOutputIndex(1);

    data.setSession(session);
    data.setMappings(new Mapping[] {mapping});
    data.setOutputRowMeta(outputMeta);
    transform.first = false;

    when(commands.hashGet(any(byte[].class), any(byte[].class)))
        .thenReturn("a@example.com".getBytes(StandardCharsets.UTF_8));
    doReturn(inputMeta).when(transform).getInputRowMeta();
    doReturn("profile").when(transform).resolve("profile");
    doReturn("email").when(transform).resolve("email");
    doReturn(new Object[] {"x"}).when(transform).getRow();
    ArgumentCaptor<Object[]> outCaptor = ArgumentCaptor.forClass(Object[].class);
    doAnswer(inv -> null).when(transform).putRow(any(), outCaptor.capture());

    assertTrue(transform.processRow());
    verify(commands)
        .hashGet(
            eq("profile".getBytes(StandardCharsets.UTF_8)),
            eq("email".getBytes(StandardCharsets.UTF_8)));
    assertEquals("a@example.com", outCaptor.getValue()[1]);
  }

  @Test
  void processRowReadsSetAsJsonArray() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    RedisInputData data = new RedisInputData();
    RedisInput transform = spy(createTransform(meta, data));

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("k"));
    IRowMeta outputMeta = inputMeta.clone();
    outputMeta.addValueMeta(new ValueMetaString("members"));

    Mapping mapping = new Mapping();
    mapping.setStructure(RedisDataStructure.SET);
    mapping.setKeyFieldIndex(0);
    mapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueOutputIndex(1);

    data.setSession(session);
    data.setMappings(new Mapping[] {mapping});
    data.setOutputRowMeta(outputMeta);
    transform.first = false;

    when(commands.setMembers(any(byte[].class)))
        .thenReturn(Set.of("bob".getBytes(StandardCharsets.UTF_8)));
    doReturn(inputMeta).when(transform).getInputRowMeta();
    doReturn(new Object[] {"team"}).when(transform).getRow();
    ArgumentCaptor<Object[]> outCaptor = ArgumentCaptor.forClass(Object[].class);
    doAnswer(inv -> null).when(transform).putRow(any(), outCaptor.capture());

    assertTrue(transform.processRow());
    assertEquals("[\"bob\"]", outCaptor.getValue()[1]);
  }

  @Test
  void processRowReadsListRangeAsJsonArray() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    RedisInputData data = new RedisInputData();
    RedisInput transform = spy(createTransform(meta, data));

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("k"));
    IRowMeta outputMeta = inputMeta.clone();
    outputMeta.addValueMeta(new ValueMetaString("items"));

    Mapping mapping = new Mapping();
    mapping.setStructure(RedisDataStructure.LIST);
    mapping.setKeyFieldIndex(0);
    mapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueOutputIndex(1);
    mapping.setListStart(0);
    mapping.setListStop(1);

    data.setSession(session);
    data.setMappings(new Mapping[] {mapping});
    data.setOutputRowMeta(outputMeta);
    transform.first = false;

    when(commands.listRange(any(byte[].class), eq(0L), eq(1L)))
        .thenReturn(
            List.of("a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8)));
    doReturn(inputMeta).when(transform).getInputRowMeta();
    doReturn(new Object[] {"queue"}).when(transform).getRow();
    ArgumentCaptor<Object[]> outCaptor = ArgumentCaptor.forClass(Object[].class);
    doAnswer(inv -> null).when(transform).putRow(any(), outCaptor.capture());

    assertTrue(transform.processRow());
    verify(commands).listRange(any(byte[].class), eq(0L), eq(1L));
    assertEquals("[\"a\",\"b\"]", outCaptor.getValue()[1]);
  }

  @Test
  void processRowReturnsNullWhenStringMissing() throws Exception {
    RedisInputMeta meta = new RedisInputMeta();
    RedisInputData data = new RedisInputData();
    RedisInput transform = spy(createTransform(meta, data));

    IRowMeta inputMeta = new RowMeta();
    inputMeta.addValueMeta(new ValueMetaString("k"));
    IRowMeta outputMeta = inputMeta.clone();
    outputMeta.addValueMeta(new ValueMetaString("v"));

    data.setSession(session);
    data.setMappings(new Mapping[] {stringMapping(0, 1)});
    data.setOutputRowMeta(outputMeta);
    transform.first = false;

    when(commands.getValue(any(byte[].class))).thenReturn(null);
    doReturn(inputMeta).when(transform).getInputRowMeta();
    doReturn(new Object[] {"missing"}).when(transform).getRow();
    ArgumentCaptor<Object[]> outCaptor = ArgumentCaptor.forClass(Object[].class);
    doAnswer(inv -> null).when(transform).putRow(any(), outCaptor.capture());

    assertTrue(transform.processRow());
    assertNull(outCaptor.getValue()[1]);
  }

  private RedisInput createTransform(RedisInputMeta meta, RedisInputData data) {
    return new RedisInput(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private static Mapping stringMapping(int keyFieldIndex, int valueOutputIndex) {
    Mapping mapping = new Mapping();
    mapping.setStructure(RedisDataStructure.STRING);
    mapping.setKeyFieldIndex(keyFieldIndex);
    mapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueOutputIndex(valueOutputIndex);
    return mapping;
  }

  private static byte[] captureKey(IRedisCommands commands) {
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(commands).getValue(keyCaptor.capture());
    return keyCaptor.getValue();
  }
}
