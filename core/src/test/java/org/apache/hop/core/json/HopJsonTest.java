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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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

  public static class Simple {
    public int a;
  }
}
