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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
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
import org.apache.hop.redis.transforms.redisinput.RedisInputData.Mapping;

public class RedisInput extends BaseTransform<RedisInputMeta, RedisInputData> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public RedisInput(
      TransformMeta transformMeta,
      RedisInputMeta meta,
      RedisInputData data,
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

      IRedisCommands commands = data.session.getCommands();
      Object[] output = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());

      for (Mapping mapping : data.mappings) {
        Object keyObj =
            mapping.keyFieldIndex >= 0 ? row[mapping.keyFieldIndex] : resolve(mapping.keyLiteral);
        byte[] keyBytes = mapping.keyCodec.encode(keyObj);
        output[mapping.valueOutputIndex] = readValue(commands, mapping, row, keyBytes);
      }

      putRow(data.outputRowMeta, output);
      return true;
    } catch (Exception e) {
      throw new HopException("Error reading from Redis", e);
    }
  }

  private void initFirst() throws HopException {
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    data.firstValueIndex = getInputRowMeta().size();

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

    List<RedisInputField> fields = meta.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new HopException("At least one field mapping is required");
    }

    data.mappings = new Mapping[fields.size()];
    int valueSlot = 0;
    for (int i = 0; i < fields.size(); i++) {
      RedisInputField field = fields.get(i);
      int rowNum = i + 1;
      if (StringUtils.isEmpty(field.getRedisKey())) {
        throw new HopException("Redis key is required for mapping row " + rowNum);
      }
      if (StringUtils.isEmpty(field.getValueField())) {
        throw new HopException("Value field is required for mapping row " + rowNum);
      }

      Mapping mapping = new Mapping();
      mapping.structure = field.resolveDataStructure();
      mapping.keyCodec = RedisCodecs.create(field.getRedisKeyCodec());
      mapping.valueCodec = RedisCodecs.create(field.getValueCodec());
      mapping.valueOutputIndex = data.firstValueIndex + valueSlot;
      valueSlot++;

      String keyExpr = field.getRedisKey();
      int keyIndex = getInputRowMeta().indexOfValue(keyExpr);
      if (keyIndex >= 0) {
        mapping.keyFieldIndex = keyIndex;
      } else {
        mapping.keyLiteral = keyExpr;
      }

      if (mapping.structure == RedisDataStructure.HASH) {
        if (StringUtils.isEmpty(field.getHashField())) {
          throw new HopException("Hash field is required for HASH mapping row " + rowNum);
        }
        mapping.hashFieldCodec = RedisCodecs.create(field.getHashFieldCodec());
        String hashExpr = field.getHashField();
        int hashIndex = getInputRowMeta().indexOfValue(hashExpr);
        if (hashIndex >= 0) {
          mapping.hashFieldIndex = hashIndex;
        } else {
          mapping.hashFieldLiteral = hashExpr;
        }
      }

      if (mapping.structure == RedisDataStructure.LIST) {
        mapping.listStart = Const.toLong(resolve(field.resolveListStart()), 0L);
        mapping.listStop = Const.toLong(resolve(field.resolveListStop()), -1L);
      }

      data.mappings[i] = mapping;
    }
  }

  private Object readValue(IRedisCommands commands, Mapping mapping, Object[] row, byte[] keyBytes)
      throws Exception {
    return switch (mapping.structure) {
      case STRING -> {
        byte[] value = commands.getValue(keyBytes);
        yield value == null ? null : mapping.valueCodec.decode(value);
      }
      case HASH -> {
        Object hashObj =
            mapping.hashFieldIndex >= 0
                ? row[mapping.hashFieldIndex]
                : resolve(mapping.hashFieldLiteral);
        byte[] fieldBytes = mapping.hashFieldCodec.encode(hashObj);
        byte[] value = commands.hashGet(keyBytes, fieldBytes);
        yield value == null ? null : mapping.valueCodec.decode(value);
      }
      case SET -> {
        Set<byte[]> members = commands.setMembers(keyBytes);
        yield toJsonArray(members == null ? List.of() : new ArrayList<>(members), mapping);
      }
      case LIST -> {
        List<byte[]> elements = commands.listRange(keyBytes, mapping.listStart, mapping.listStop);
        yield toJsonArray(elements == null ? List.of() : elements, mapping);
      }
    };
  }

  /**
   * Decode each Redis value with the mapping codec, then serialize as a JSON array string for Hop
   * row compatibility. {@code byte[]} from the BYTE codec is converted to UTF-8 text when valid,
   * otherwise to a Base64 string (Jackson would otherwise emit opaque Base64 for raw byte arrays).
   */
  private Object toJsonArray(List<byte[]> rawValues, Mapping mapping) throws Exception {
    List<Object> decoded = new ArrayList<>(rawValues.size());
    for (byte[] raw : rawValues) {
      if (raw == null) {
        decoded.add(null);
        continue;
      }
      decoded.add(toJsonArrayElement(mapping.valueCodec.decode(raw)));
    }
    return OBJECT_MAPPER.writeValueAsString(decoded);
  }

  static Object toJsonArrayElement(Object decoded) {
    if (!(decoded instanceof byte[] bytes)) {
      return decoded;
    }
    if (isUtf8Text(bytes)) {
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static boolean isUtf8Text(byte[] bytes) {
    CharsetDecoder decoder =
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      decoder.decode(java.nio.ByteBuffer.wrap(bytes));
      return true;
    } catch (Exception e) {
      return false;
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
}
