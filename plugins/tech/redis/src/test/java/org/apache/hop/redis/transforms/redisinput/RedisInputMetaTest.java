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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.redis.codec.RedisCodecType;
import org.apache.hop.redis.transforms.RedisDataStructure;
import org.junit.jupiter.api.Test;

class RedisInputMetaTest {

  @Test
  void testXmlRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList("connectionName", "fields");

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidators = new HashMap<>();
    attrValidators.put("fields", new ListLoadSaveValidator<>(new RedisInputFieldValidator(), 3));

    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<>();

    LoadSaveTester tester =
        new LoadSaveTester(
            RedisInputMeta.class, attributes, getterMap, setterMap, attrValidators, typeValidators);
    tester.testXmlRoundTrip();
  }

  private static final class RedisInputFieldValidator
      implements IFieldLoadSaveValidator<RedisInputField> {
    private final Random random = new Random();

    @Override
    public RedisInputField getTestObject() {
      RedisCodecType[] codecs = RedisCodecType.values();
      RedisDataStructure[] structures = RedisDataStructure.values();
      RedisInputField field = new RedisInputField();
      field.setRedisKey("key_" + UUID.randomUUID());
      field.setRedisKeyCodec(codecs[random.nextInt(codecs.length)]);
      field.setDataStructure(structures[random.nextInt(structures.length)]);
      field.setHashField(random.nextBoolean() ? "hash_" + UUID.randomUUID() : "");
      field.setHashFieldCodec(random.nextBoolean() ? codecs[random.nextInt(codecs.length)] : null);
      field.setValueField("value_" + UUID.randomUUID());
      field.setValueCodec(codecs[random.nextInt(codecs.length)]);
      field.setListStart(random.nextBoolean() ? "0" : String.valueOf(random.nextInt(10)));
      field.setListStop(random.nextBoolean() ? "-1" : String.valueOf(random.nextInt(10)));
      return field;
    }

    @Override
    public boolean validateTestObject(RedisInputField testObject, Object actual) {
      if (!(actual instanceof RedisInputField other)) {
        return false;
      }

      return Objects.equals(testObject.getRedisKey(), other.getRedisKey())
          && testObject.getRedisKeyCodec() == other.getRedisKeyCodec()
          && testObject.getDataStructure() == other.getDataStructure()
          && sameOmittedString(testObject.getHashField(), other.getHashField())
          && testObject.getHashFieldCodec() == other.getHashFieldCodec()
          && Objects.equals(testObject.getValueField(), other.getValueField())
          && testObject.getValueCodec() == other.getValueCodec()
          && Objects.equals(testObject.getListStart(), other.getListStart())
          && Objects.equals(testObject.getListStop(), other.getListStop());
    }

    /** Empty strings are omitted from XML and come back as null. */
    private static boolean sameOmittedString(String expected, String actual) {
      return StringUtils.isEmpty(expected)
          ? StringUtils.isEmpty(actual)
          : Objects.equals(expected, actual);
    }
  }
}
