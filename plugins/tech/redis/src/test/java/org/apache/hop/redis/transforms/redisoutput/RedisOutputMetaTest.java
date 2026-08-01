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
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.apache.hop.redis.transforms.RedisListPushDirection;
import org.junit.jupiter.api.Test;

class RedisOutputMetaTest {

  @Test
  void testXmlRoundTrip() throws HopException {
    List<String> attributes =
        Arrays.asList(
            "connectionName",
            "writeMode",
            "dataStructure",
            "keyCodec",
            "valueCodec",
            "hashKeyCodec",
            "hashValueCodec",
            "keyField",
            "valueField",
            "hashKeyField",
            "hashValueField",
            "ttlSeconds",
            "listPushDirection",
            "fields");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidators = new HashMap<>();
    attrValidators.put("writeMode", new EnumLoadSaveValidator<>(RedisOutputWriteMode.class));
    attrValidators.put("dataStructure", new EnumLoadSaveValidator<>(RedisDataStructure.class));
    attrValidators.put("keyCodec", new EnumLoadSaveValidator<>(RedisCodecType.class));
    attrValidators.put("valueCodec", new EnumLoadSaveValidator<>(RedisCodecType.class));
    attrValidators.put("hashKeyCodec", new EnumLoadSaveValidator<>(RedisCodecType.class));
    attrValidators.put("hashValueCodec", new EnumLoadSaveValidator<>(RedisCodecType.class));
    attrValidators.put(
        "listPushDirection", new EnumLoadSaveValidator<>(RedisListPushDirection.class));
    attrValidators.put("fields", new ListLoadSaveValidator<>(new RedisOutputFieldValidator(), 3));

    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<>();

    LoadSaveTester tester =
        new LoadSaveTester(
            RedisOutputMeta.class,
            attributes,
            getterMap,
            setterMap,
            attrValidators,
            typeValidators);
    tester.testXmlRoundTrip();
  }

  private static final class RedisOutputFieldValidator
      implements IFieldLoadSaveValidator<RedisOutputField> {
    private final Random random = new Random();

    @Override
    public RedisOutputField getTestObject() {
      RedisCodecType[] codecs = RedisCodecType.values();
      RedisDataStructure[] structures = RedisDataStructure.values();
      RedisOutputField field = new RedisOutputField();
      field.setStreamField("stream_" + UUID.randomUUID());
      field.setDataStructure(structures[random.nextInt(structures.length)]);
      field.setKey("key_" + UUID.randomUUID());
      field.setKeyCodec(codecs[random.nextInt(codecs.length)]);
      field.setHashKey(random.nextBoolean() ? "hash_" + UUID.randomUUID() : "");
      field.setHashKeyCodec(random.nextBoolean() ? codecs[random.nextInt(codecs.length)] : null);
      field.setValueCodec(codecs[random.nextInt(codecs.length)]);
      field.setTtlSeconds(random.nextBoolean() ? "0" : String.valueOf(random.nextInt(3600) + 1));
      if (random.nextBoolean()) {
        field.setRedisName("legacy_" + UUID.randomUUID());
      }
      return field;
    }

    @Override
    public boolean validateTestObject(RedisOutputField testObject, Object actual) {
      if (!(actual instanceof RedisOutputField other)) {
        return false;
      }
      return Objects.equals(testObject.getStreamField(), other.getStreamField())
          && testObject.getDataStructure() == other.getDataStructure()
          && Objects.equals(testObject.getKey(), other.getKey())
          && testObject.getKeyCodec() == other.getKeyCodec()
          && Objects.equals(testObject.getHashKey(), other.getHashKey())
          && testObject.getHashKeyCodec() == other.getHashKeyCodec()
          && testObject.getValueCodec() == other.getValueCodec()
          && Objects.equals(testObject.getTtlSeconds(), other.getTtlSeconds())
          && Objects.equals(testObject.getRedisName(), other.getRedisName());
    }
  }
}
