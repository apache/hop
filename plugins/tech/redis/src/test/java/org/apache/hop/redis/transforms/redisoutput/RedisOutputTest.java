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

package org.apache.hop.redis.transforms.redisoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;
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
import org.apache.hop.redis.transforms.RedisListPushDirection;
import org.apache.hop.redis.transforms.redisoutput.RedisOutputData.StreamMapping;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RedisOutputTest {

  private TransformMockHelper<RedisOutputMeta, RedisOutputData> mockHelper;
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
        new TransformMockHelper<>("RedisOutput", RedisOutputMeta.class, RedisOutputData.class);
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
  void keyValueWritesStringAndExpires() throws Exception {
    RedisOutputMeta meta = new RedisOutputMeta();
    meta.setWriteMode(RedisOutputWriteMode.KEY_VALUE);
    meta.setDataStructure(RedisDataStructure.STRING);
    meta.setKeyField("key");
    meta.setValueField("value");

    RedisOutputData data = preparedKeyValueData(0, 1, -1, -1, 30L);
    RedisOutput transform = spy(createTransform(meta, data));
    stubRow(transform, data, new Object[] {"user:1", "alice"});

    assertTrue(transform.processRow());
    verify(commands)
        .setValue(
            eq("user:1".getBytes(StandardCharsets.UTF_8)),
            eq("alice".getBytes(StandardCharsets.UTF_8)));
    verify(commands).expire(eq("user:1".getBytes(StandardCharsets.UTF_8)), eq(30L));
  }

  @Test
  void keyValueSkipsExpireWhenTtlMissing() throws Exception {
    RedisOutputMeta meta = new RedisOutputMeta();
    meta.setWriteMode(RedisOutputWriteMode.KEY_VALUE);
    meta.setDataStructure(RedisDataStructure.STRING);
    meta.setKeyField("key");
    meta.setValueField("value");

    RedisOutputData data = preparedKeyValueData(0, 1, -1, -1, null);
    RedisOutput transform = spy(createTransform(meta, data));
    stubRow(transform, data, new Object[] {"user:1", "alice"});

    assertTrue(transform.processRow());
    verify(commands).setValue(any(byte[].class), any(byte[].class));
    verify(commands, never()).expire(any(byte[].class), anyLong());
  }

  @Test
  void keyValueWritesHash() throws Exception {
    RedisOutputMeta hashMeta = new RedisOutputMeta();
    hashMeta.setWriteMode(RedisOutputWriteMode.KEY_VALUE);
    hashMeta.setDataStructure(RedisDataStructure.HASH);
    hashMeta.setKeyField("key");
    hashMeta.setHashKeyField("hk");
    hashMeta.setHashValueField("hv");
    RedisOutputData hashData = preparedKeyValueData(0, -1, 1, 2, null);
    RedisOutput hashTransform = spy(createTransform(hashMeta, hashData));
    stubRow(hashTransform, hashData, new Object[] {"hkey", "field", "val"});
    assertTrue(hashTransform.processRow());
    verify(commands)
        .hashSet(
            eq("hkey".getBytes(StandardCharsets.UTF_8)),
            eq("field".getBytes(StandardCharsets.UTF_8)),
            eq("val".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void keyValueWritesSet() throws Exception {
    RedisOutputMeta setMeta = new RedisOutputMeta();
    setMeta.setWriteMode(RedisOutputWriteMode.KEY_VALUE);
    setMeta.setDataStructure(RedisDataStructure.SET);
    setMeta.setKeyField("key");
    setMeta.setValueField("value");
    RedisOutputData setData = preparedKeyValueData(0, 1, -1, -1, null);
    RedisOutput setTransform = spy(createTransform(setMeta, setData));
    stubRow(setTransform, setData, new Object[] {"skey", "member"});
    assertTrue(setTransform.processRow());
    verify(commands)
        .setAdd(
            eq("skey".getBytes(StandardCharsets.UTF_8)),
            eq("member".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void keyValueListUsesLpushWhenConfigured() throws Exception {
    RedisOutputMeta listMeta = new RedisOutputMeta();
    listMeta.setWriteMode(RedisOutputWriteMode.KEY_VALUE);
    listMeta.setDataStructure(RedisDataStructure.LIST);
    listMeta.setKeyField("key");
    listMeta.setValueField("value");
    listMeta.setListPushDirection(RedisListPushDirection.LPUSH);
    RedisOutputData listData = preparedKeyValueData(0, 1, -1, -1, null);
    RedisOutput listTransform = spy(createTransform(listMeta, listData));
    stubRow(listTransform, listData, new Object[] {"lkey", "elem"});
    assertTrue(listTransform.processRow());
    verify(commands)
        .listLeftPush(
            eq("lkey".getBytes(StandardCharsets.UTF_8)),
            eq("elem".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void streamFieldsBatchesHashWritesForSameKey() throws Exception {
    RedisOutputMeta meta = new RedisOutputMeta();
    meta.setWriteMode(RedisOutputWriteMode.STREAM_FIELDS);

    RedisOutputData data = new RedisOutputData();
    data.setSession(session);
    data.setOutputRowMeta(rowMeta("name", "age"));
    data.setCodecs(
        RedisCodecs.of(
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING));

    StreamMapping nameMapping = hashStreamMapping(0, "user", "name", 10L);
    StreamMapping ageMapping = hashStreamMapping(1, "user", "age", 5L);
    data.setStreamMappings(new StreamMapping[] {nameMapping, ageMapping});

    RedisOutput transform = spy(createTransform(meta, data));
    transform.first = false;
    doReturn(data.getOutputRowMeta()).when(transform).getInputRowMeta();
    doReturn(new Object[] {"alice", "30"}).when(transform).getRow();
    doAnswer(inv -> null).when(transform).putRow(any(), any());
    doReturn("user").when(transform).resolve("user");
    doReturn("name").when(transform).resolve("name");
    doReturn("age").when(transform).resolve("age");

    assertTrue(transform.processRow());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<byte[], byte[]>> mapCaptor = ArgumentCaptor.forClass(Map.class);
    verify(commands).hashSet(eq("user".getBytes(StandardCharsets.UTF_8)), mapCaptor.capture());
    assertEquals(2, mapCaptor.getValue().size());
    // TTL uses max(10, 5)
    verify(commands).expire(eq("user".getBytes(StandardCharsets.UTF_8)), eq(10L));
  }

  @Test
  void streamFieldsWritesStringSetAndDefaultListRpush() throws Exception {
    RedisOutputMeta meta = new RedisOutputMeta();
    meta.setWriteMode(RedisOutputWriteMode.STREAM_FIELDS);

    RedisOutputData data = new RedisOutputData();
    data.setSession(session);
    data.setOutputRowMeta(rowMeta("v"));
    data.setCodecs(
        RedisCodecs.of(
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING));

    StreamMapping stringMapping = new StreamMapping();
    stringMapping.setStructure(RedisDataStructure.STRING);
    stringMapping.setStreamFieldIndex(0);
    stringMapping.setKeyLiteral("s");
    stringMapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    stringMapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));

    StreamMapping setMapping = new StreamMapping();
    setMapping.setStructure(RedisDataStructure.SET);
    setMapping.setStreamFieldIndex(0);
    setMapping.setKeyLiteral("set");
    setMapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    setMapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));

    StreamMapping listMapping = new StreamMapping();
    listMapping.setStructure(RedisDataStructure.LIST);
    listMapping.setStreamFieldIndex(0);
    listMapping.setKeyLiteral("list");
    listMapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    listMapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));

    data.setStreamMappings(new StreamMapping[] {stringMapping, setMapping, listMapping});

    RedisOutput transform = spy(createTransform(meta, data));
    transform.first = false;
    doReturn(data.getOutputRowMeta()).when(transform).getInputRowMeta();
    doReturn(new Object[] {"x"}).when(transform).getRow();
    doAnswer(inv -> null).when(transform).putRow(any(), any());
    doReturn("s").when(transform).resolve("s");
    doReturn("set").when(transform).resolve("set");
    doReturn("list").when(transform).resolve("list");

    assertTrue(transform.processRow());
    verify(commands)
        .setValue(
            eq("s".getBytes(StandardCharsets.UTF_8)), eq("x".getBytes(StandardCharsets.UTF_8)));
    verify(commands)
        .setAdd(
            eq("set".getBytes(StandardCharsets.UTF_8)), eq("x".getBytes(StandardCharsets.UTF_8)));
    verify(commands)
        .listRightPush(
            eq("list".getBytes(StandardCharsets.UTF_8)), eq("x".getBytes(StandardCharsets.UTF_8)));
  }

  private RedisOutput createTransform(RedisOutputMeta meta, RedisOutputData data) {
    return new RedisOutput(
        mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  private RedisOutputData preparedKeyValueData(
      int keyIdx, int valueIdx, int hashKeyIdx, int hashValueIdx, Long ttl) {
    RedisOutputData data = new RedisOutputData();
    data.setSession(session);
    data.setCodecs(
        RedisCodecs.of(
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING,
            RedisCodecType.STRING));
    data.setKeyFieldIndex(keyIdx);
    data.setValueFieldIndex(valueIdx);
    data.setHashKeyFieldIndex(hashKeyIdx);
    data.setHashValueFieldIndex(hashValueIdx);
    data.setTtlSeconds(ttl);
    data.setOutputRowMeta(rowMeta("key", "value", "hk", "hv"));
    return data;
  }

  private void stubRow(RedisOutput transform, RedisOutputData data, Object[] row) throws Exception {
    transform.first = false;
    doReturn(data.getOutputRowMeta()).when(transform).getInputRowMeta();
    doReturn(row).when(transform).getRow();
    doAnswer(inv -> null).when(transform).putRow(any(), any());
  }

  private static IRowMeta rowMeta(String... names) {
    RowMeta meta = new RowMeta();
    for (String name : names) {
      meta.addValueMeta(new ValueMetaString(name));
    }
    return meta;
  }

  private static StreamMapping hashStreamMapping(
      int streamIndex, String keyLiteral, String hashKeyLiteral, Long ttl) {
    StreamMapping mapping = new StreamMapping();
    mapping.setStructure(RedisDataStructure.HASH);
    mapping.setStreamFieldIndex(streamIndex);
    mapping.setKeyLiteral(keyLiteral);
    mapping.setKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setHashKeyLiteral(hashKeyLiteral);
    mapping.setHashKeyCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setValueCodec(RedisCodecs.create(RedisCodecType.STRING));
    mapping.setTtlSeconds(ttl);
    return mapping;
  }
}
