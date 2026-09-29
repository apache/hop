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
 *
 */

package org.apache.hop.core.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class HopJson {

  /**
   * Jackson's default maximum string length is 20,000,000 characters (5,000,000 in 2.15.0). Sampled
   * execution rows are stored as one Base64 string and need a higher limit to be read back.
   */
  public static final int MAX_STRING_LENGTH = 50_000_000;

  private HopJson() {}

  /**
   * @return a factory that accepts a string of up to {@link #MAX_STRING_LENGTH} characters
   */
  public static JsonFactory newFactory() {
    return new JsonFactoryBuilder()
        .streamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(MAX_STRING_LENGTH).build())
        .build();
  }

  /**
   * @return a new ObjectMapper with the default options set for Hop file serialization and
   *     de-serialization
   */
  public static final ObjectMapper newMapper() {
    ObjectMapper objectMapper = new ObjectMapper(newFactory());
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.disable(SerializationFeature.INDENT_OUTPUT);
    return objectMapper;
  }
}
