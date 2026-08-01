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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.redis.client.IRedisCommands;
import org.apache.hop.redis.client.RedisClientFactory;
import org.apache.hop.redis.codec.RedisCodecs;
import org.apache.hop.redis.metadata.RedisConnection;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.RedisListPushDirection;
import org.apache.hop.redis.transforms.redisoutput.RedisOutputData.StreamMapping;

public class RedisOutput extends BaseTransform<RedisOutputMeta, RedisOutputData> {

  public RedisOutput(
      TransformMeta transformMeta,
      RedisOutputMeta meta,
      RedisOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    try {
      if (first) {
        first = false;
        initFirst();
      }

      RedisOutputWriteMode writeMode =
          meta.getWriteMode() == null ? RedisOutputWriteMode.KEY_VALUE : meta.getWriteMode();
      if (writeMode == RedisOutputWriteMode.STREAM_FIELDS) {
        writeStreamFields(row);
      } else {
        writeKeyValue(row);
      }

      putRow(data.outputRowMeta, row);
      return true;
    } catch (Exception e) {
      throw new HopException("Error writing to Redis", e);
    }
  }

  private void initFirst() throws HopException {
    data.outputRowMeta = getInputRowMeta().clone();

    if (StringUtils.isEmpty(meta.getConnectionName())) {
      throw new HopException("A Redis connection name is required");
    }
    IHopMetadataSerializer<RedisConnection> serializer =
        metadataProvider.getSerializer(RedisConnection.class);
    RedisConnection connection = serializer.load(meta.getConnectionName());
    if (connection == null) {
      throw new HopException("Redis connection '" + meta.getConnectionName() + "' not found");
    }
    data.session = RedisClientFactory.create(connection, this);
    data.codecs =
        RedisCodecs.of(
            meta.getKeyCodec(),
            meta.getValueCodec(),
            meta.getHashKeyCodec(),
            meta.getHashValueCodec());

    RedisOutputWriteMode writeMode =
        meta.getWriteMode() == null ? RedisOutputWriteMode.KEY_VALUE : meta.getWriteMode();
    if (writeMode == RedisOutputWriteMode.STREAM_FIELDS) {
      initStreamFields();
    } else {
      RedisDataStructure structure =
          meta.getDataStructure() == null ? RedisDataStructure.STRING : meta.getDataStructure();
      initKeyValue(structure);
      // Component-level TTL only applies to KEY_VALUE mode
      String ttl = resolve(meta.getTtlSeconds());
      if (StringUtils.isNotEmpty(ttl)) {
        long ttlSeconds = Const.toLong(ttl, 0L);
        if (ttlSeconds > 0) {
          data.ttlSeconds = ttlSeconds;
        }
      }
    }
  }

  private void initKeyValue(RedisDataStructure structure) throws HopException {
    data.keyFieldIndex = getInputRowMeta().indexOfValue(meta.getKeyField());
    if (data.keyFieldIndex < 0) {
      throw new HopException("Unable to find key field '" + meta.getKeyField() + "'");
    }

    switch (structure) {
      case STRING, SET, LIST -> {
        data.valueFieldIndex = getInputRowMeta().indexOfValue(meta.getValueField());
        if (data.valueFieldIndex < 0) {
          throw new HopException("Unable to find value field '" + meta.getValueField() + "'");
        }
      }
      case HASH -> {
        data.hashKeyFieldIndex = getInputRowMeta().indexOfValue(meta.getHashKeyField());
        data.hashValueFieldIndex = getInputRowMeta().indexOfValue(meta.getHashValueField());
        if (data.hashKeyFieldIndex < 0) {
          throw new HopException("Unable to find hash key field '" + meta.getHashKeyField() + "'");
        }
        if (data.hashValueFieldIndex < 0) {
          throw new HopException(
              "Unable to find hash value field '" + meta.getHashValueField() + "'");
        }
      }
    }
  }

  private void initStreamFields() throws HopException {
    List<RedisOutputField> fields = meta.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new HopException("At least one stream field is required in STREAM_FIELDS mode");
    }

    data.streamMappings = new StreamMapping[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      RedisOutputField field = fields.get(i);
      int rowNum = i + 1;
      if (StringUtils.isEmpty(field.getStreamField())) {
        throw new HopException("Stream field is required for mapping row " + rowNum);
      }
      int streamIndex = getInputRowMeta().indexOfValue(field.getStreamField());
      if (streamIndex < 0) {
        throw new HopException("Unable to find stream field '" + field.getStreamField() + "'");
      }

      String keyExpr = field.resolveKey();
      if (StringUtils.isEmpty(keyExpr)) {
        throw new HopException("Key is required for mapping row " + rowNum);
      }

      StreamMapping mapping = new StreamMapping();
      mapping.structure = field.resolveDataStructure();
      mapping.streamFieldIndex = streamIndex;
      mapping.keyCodec = RedisCodecs.create(field.getKeyCodec());
      mapping.valueCodec = RedisCodecs.create(field.getValueCodec());

      int keyIndex = getInputRowMeta().indexOfValue(keyExpr);
      if (keyIndex >= 0) {
        mapping.keyFieldIndex = keyIndex;
      } else {
        mapping.keyLiteral = keyExpr;
      }

      if (mapping.structure == RedisDataStructure.HASH) {
        String hashKeyExpr = field.resolveHashKey();
        if (StringUtils.isEmpty(hashKeyExpr)) {
          throw new HopException("Hash key is required for HASH mapping row " + rowNum);
        }
        mapping.hashKeyCodec = RedisCodecs.create(field.getHashKeyCodec());
        int hashKeyIndex = getInputRowMeta().indexOfValue(hashKeyExpr);
        if (hashKeyIndex >= 0) {
          mapping.hashKeyFieldIndex = hashKeyIndex;
        } else {
          mapping.hashKeyLiteral = hashKeyExpr;
        }
      }

      mapping.ttlSeconds = resolveTtlSeconds(field.getTtlSeconds());
      data.streamMappings[i] = mapping;
    }
  }

  private Long resolveTtlSeconds(String ttlExpression) {
    if (StringUtils.isEmpty(ttlExpression)) {
      return null;
    }
    long ttlSeconds = Const.toLong(resolve(ttlExpression), 0L);
    return ttlSeconds > 0 ? ttlSeconds : null;
  }

  private void writeKeyValue(Object[] row) throws Exception {
    IRedisCommands commands = data.session.getCommands();
    byte[] keyBytes = data.codecs.key().encode(row[data.keyFieldIndex]);
    RedisDataStructure structure =
        meta.getDataStructure() == null ? RedisDataStructure.STRING : meta.getDataStructure();

    switch (structure) {
      case STRING -> {
        byte[] valueBytes = data.codecs.value().encode(row[data.valueFieldIndex]);
        commands.setValue(keyBytes, valueBytes);
      }
      case HASH -> {
        byte[] fieldBytes = data.codecs.hashKey().encode(row[data.hashKeyFieldIndex]);
        byte[] valueBytes = data.codecs.hashValue().encode(row[data.hashValueFieldIndex]);
        commands.hashSet(keyBytes, fieldBytes, valueBytes);
      }
      case SET -> {
        byte[] memberBytes = data.codecs.value().encode(row[data.valueFieldIndex]);
        commands.setAdd(keyBytes, memberBytes);
      }
      case LIST -> {
        byte[] elementBytes = data.codecs.value().encode(row[data.valueFieldIndex]);
        pushList(commands, keyBytes, elementBytes);
      }
    }

    expireIfNeeded(commands, keyBytes);
  }

  private void writeStreamFields(Object[] row) throws Exception {
    IRedisCommands commands = data.session.getCommands();
    // Batch HASH fields that share the same Redis key within this pipeline row
    Map<KeyRef, Map<byte[], byte[]>> hashBatches = new HashMap<>();
    Map<KeyRef, Long> hashTtls = new HashMap<>();

    for (StreamMapping mapping : data.streamMappings) {
      Object keyObj =
          mapping.keyFieldIndex >= 0 ? row[mapping.keyFieldIndex] : resolve(mapping.keyLiteral);
      byte[] keyBytes = mapping.keyCodec.encode(keyObj);
      byte[] valueBytes = mapping.valueCodec.encode(row[mapping.streamFieldIndex]);

      switch (mapping.structure) {
        case STRING -> {
          commands.setValue(keyBytes, valueBytes);
          expireIfNeeded(commands, keyBytes, mapping.ttlSeconds);
        }
        case HASH -> {
          Object hashKeyObj =
              mapping.hashKeyFieldIndex >= 0
                  ? row[mapping.hashKeyFieldIndex]
                  : resolve(mapping.hashKeyLiteral);
          byte[] hashKeyBytes = mapping.hashKeyCodec.encode(hashKeyObj);
          KeyRef keyRef = new KeyRef(keyBytes);
          hashBatches.computeIfAbsent(keyRef, k -> new HashMap<>()).put(hashKeyBytes, valueBytes);
          if (mapping.ttlSeconds != null) {
            hashTtls.merge(keyRef, mapping.ttlSeconds, Math::max);
          }
        }
        case SET -> {
          commands.setAdd(keyBytes, valueBytes);
          expireIfNeeded(commands, keyBytes, mapping.ttlSeconds);
        }
        case LIST -> {
          // List push direction is not configured in STREAM_FIELDS yet; default RPUSH
          commands.listRightPush(keyBytes, valueBytes);
          expireIfNeeded(commands, keyBytes, mapping.ttlSeconds);
        }
      }
    }

    for (Map.Entry<KeyRef, Map<byte[], byte[]>> entry : hashBatches.entrySet()) {
      commands.hashSet(entry.getKey().bytes, entry.getValue());
      expireIfNeeded(commands, entry.getKey().bytes, hashTtls.get(entry.getKey()));
    }
  }

  private void pushList(IRedisCommands commands, byte[] keyBytes, byte[] elementBytes) {
    RedisListPushDirection direction =
        meta.getListPushDirection() == null
            ? RedisListPushDirection.RPUSH
            : meta.getListPushDirection();
    if (direction == RedisListPushDirection.LPUSH) {
      commands.listLeftPush(keyBytes, elementBytes);
    } else {
      commands.listRightPush(keyBytes, elementBytes);
    }
  }

  private void expireIfNeeded(IRedisCommands commands, byte[] keyBytes) {
    expireIfNeeded(commands, keyBytes, data.ttlSeconds);
  }

  private void expireIfNeeded(IRedisCommands commands, byte[] keyBytes, Long ttlSeconds) {
    if (ttlSeconds != null && ttlSeconds > 0) {
      commands.expire(keyBytes, ttlSeconds);
    }
  }

  @Override
  public void dispose() {
    if (data.session != null) {
      data.session.close();
      data.session = null;
    }
    super.dispose();
  }

  /** Wrapper so HASH batches can group by key bytes. */
  private record KeyRef(byte[] bytes) {
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof KeyRef other)) {
        return false;
      }
      return Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes);
    }
  }
}
