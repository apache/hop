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

package org.apache.hop.core.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HopJsonTest {

  @Test
  void newMapperDisablesUnknownPropertyFailureAndIndent() {
    ObjectMapper mapper = HopJson.newMapper();
    assertFalse(
        mapper
            .getDeserializationConfig()
            .isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
    assertFalse(mapper.getSerializationConfig().isEnabled(SerializationFeature.INDENT_OUTPUT));
  }

  @Test
  void newMapperIgnoresUnknownJsonProperties() throws Exception {
    ObjectMapper mapper = HopJson.newMapper();
    Simple bean = mapper.readValue("{\"a\":1,\"extra\":true}", Simple.class);
    assertEquals(1, bean.a);
  }

  @Test
  void newMapperWritesCompactJson() throws Exception {
    ObjectMapper mapper = HopJson.newMapper();
    Simple bean = new Simple();
    bean.a = 42;
    String json = mapper.writeValueAsString(bean);
    assertFalse(json.contains("\n"));
    assertEquals("{\"a\":42}", json);
  }

  @Test
  void newMapperReadsAStringLongerThanTheJacksonDefault() throws Exception {
    int length = StreamReadConstraints.DEFAULT_MAX_STRING_LEN + 1;
    String json = "{\"rowsBinaryGzipBase64Encoded\":\"" + "x".repeat(length) + "\"}";

    JsonProcessingException rejected =
        assertThrows(
            JsonProcessingException.class, () -> new ObjectMapper().readValue(json, Map.class));
    assertTrue(causedBy(rejected, StreamConstraintsException.class), rejected.toString());

    ObjectMapper mapper = HopJson.newMapper();
    StreamReadConstraints constraints = mapper.getFactory().streamReadConstraints();
    assertEquals(HopJson.MAX_STRING_LENGTH, constraints.getMaxStringLength());
    assertEquals(
        StreamReadConstraints.defaults().getMaxNestingDepth(), constraints.getMaxNestingDepth());
    assertEquals(
        StreamReadConstraints.defaults().getMaxNameLength(), constraints.getMaxNameLength());

    Map<?, ?> back = mapper.readValue(json, Map.class);
    assertEquals(length, String.valueOf(back.get("rowsBinaryGzipBase64Encoded")).length());

    ObjectMapper strict = new ObjectMapper(HopJson.newFactory());
    assertEquals(
        length,
        String.valueOf(strict.readValue(json, Map.class).get("rowsBinaryGzipBase64Encoded"))
            .length());
  }

  private static boolean causedBy(Throwable throwable, Class<? extends Throwable> type) {
    while (throwable != null) {
      if (type.isInstance(throwable)) {
        return true;
      }
      throwable = throwable.getCause();
    }
    return false;
  }

  public static class Simple {
    public int a;
  }
}
