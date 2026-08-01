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

package org.apache.hop.redis.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RedisCodecsTest {

  @Test
  void stringRoundTrip() throws Exception {
    RedisValueCodec codec = RedisCodecs.create(RedisCodecType.STRING);
    byte[] encoded = codec.encode("hello");
    assertEquals("hello", codec.decode(encoded));
  }

  @Test
  void jsonRoundTrip() throws Exception {
    RedisValueCodec codec = RedisCodecs.create(RedisCodecType.JSON);
    byte[] encoded = codec.encode(Map.of("a", 1));
    Object decoded = codec.decode(encoded);
    assertNotNull(decoded);
    assertEquals(true, decoded.toString().contains("\"a\""));
    assertEquals(true, decoded.toString().contains("1"));
  }

  @Test
  void javaObjectRoundTrip() throws Exception {
    RedisValueCodec codec = RedisCodecs.create(RedisCodecType.JAVA_OBJECT);
    byte[] encoded = codec.encode("serializable-string");
    assertEquals("serializable-string", codec.decode(encoded));
  }

  @Test
  void byteCodecDecodesToBytes() throws Exception {
    RedisValueCodec codec = RedisCodecs.create(RedisCodecType.BYTE);
    byte[] raw = "abc".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(raw, (byte[]) codec.decode(raw));
    byte[] fromBase64 = codec.encode(Base64.getEncoder().encodeToString(raw));
    assertArrayEquals(raw, fromBase64);
  }

  @Test
  void byteCodecDoesNotTreatShortNumbersAsBase64() throws Exception {
    RedisValueCodec codec = RedisCodecs.create(RedisCodecType.BYTE);
    // "12" is technically Base64 for 0xD7 — must store as UTF-8 decimal text instead
    assertArrayEquals("12".getBytes(StandardCharsets.UTF_8), codec.encode(12));
    assertArrayEquals("12".getBytes(StandardCharsets.UTF_8), codec.encode(12L));
    assertArrayEquals("12".getBytes(StandardCharsets.UTF_8), codec.encode("12"));
  }

  @Test
  void holdersExposeFourCodecs() {
    RedisCodecs codecs =
        RedisCodecs.of(
            RedisCodecType.STRING,
            RedisCodecType.JSON,
            RedisCodecType.BYTE,
            RedisCodecType.JAVA_OBJECT);
    assertInstanceOf(RedisCodecs.StringCodec.class, codecs.key());
    assertInstanceOf(RedisCodecs.JsonCodec.class, codecs.value());
    assertInstanceOf(RedisCodecs.ByteCodec.class, codecs.hashKey());
    assertInstanceOf(RedisCodecs.JavaObjectCodec.class, codecs.hashValue());
  }
}
